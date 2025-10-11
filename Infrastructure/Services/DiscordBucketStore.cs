using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using Discord;
using Discord.Net;
using Hangfire;
using Hangfire.States;

namespace Infrastructure.Services;

public class DiscordBucketStore : IBucketStore {
    private const long MaxChunkSize = 10 * 1024 * 1024; // 10 MB
    private const int MaxFilesPerMessage = 10;

    private static readonly TimeSpan[] RetryDelays =
        { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5) };

    private readonly TimeProvider _timeProvider;
    private readonly IDbContext _db;
    private readonly DiscordMultiplexer _dcMultiplexer;
    private readonly HttpClient _httpClient;
    private readonly IRecurringJobManagerV2 _recurringJobs;
    private readonly IBackgroundJobClientV2 _jobs;
    private readonly ILogger<DiscordBucketStore> _logger;
    private readonly IStorageMetrics _metrics;

    public DiscordBucketStore(TimeProvider timeProvider, IDbContext db, DiscordMultiplexer dcMultiplexer,
        IHttpClientFactory httpClientFactory, IRecurringJobManagerV2 recurringJobs, IBackgroundJobClientV2 jobs,
        ILogger<DiscordBucketStore> logger, IStorageMetrics metrics) {
        _timeProvider = timeProvider;
        _db = db;
        _dcMultiplexer = dcMultiplexer;
        _recurringJobs = recurringJobs;
        _jobs = jobs;
        _logger = logger;
        _metrics = metrics;
        _httpClient = httpClientFactory.CreateClient(Constants.HttpClients.Discord);
    }

    public async Task<bool> AddObjectAsync(string filePath, Bucket bucket, Object obj, CancellationToken ct) {
        ArgumentNullException.ThrowIfNull(obj);
        var storageLength = obj.StorageContentLength > 0 ? obj.StorageContentLength : obj.ContentLength;
        if (storageLength <= 0) storageLength = new FileInfo(filePath).Length;
        obj.StorageContentLength = storageLength;

        var chunks = await UploadToDiscordAsync(filePath, bucket, obj, storageLength, ct);

        if (string.IsNullOrEmpty(obj.Md5)) {
            await using FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            obj.Md5 = Convert.ToHexString(await MD5.HashDataAsync(fs, ct));
        }


        if (string.IsNullOrEmpty(obj.Etag)) obj.Etag = obj.Md5;

        var ts = _timeProvider.GetUtcNow();
        obj.CreatedUtc = ts;
        obj.LastAccessUtc = ts;
        obj.LastUpdateUtc = ts;
        obj.ExpirationUtc = null;

        _db.Objects.Add(obj);
        _db.ObjectChunks.AddRange(chunks);
        var messageIds = chunks.Select(c => c.MessageId).Distinct().ToArray();
        foreach (var messageId in messageIds) {
            // refresh each message every 23 hours
            _recurringJobs.AddOrUpdate<DiscordMultiplexer>(Constants.RecurringJobs.FormatObjectRefresh(messageId),
                d => d.RefreshObjectMessageAsync(messageId, bucket.ChannelId!.Value, ct),
                $"0 0 */{ObjectChunk.ExpireAfter.Hours} * *");
        }

        await _db.SaveChangesAsync(ct);

        _metrics.RecordIngress(obj.ContentLength);
        _metrics.TrackObjectStored(storageLength);

        return true;
    }

    public Task<Stream> GetObjectStreamAsync(Bucket bucket, Object obj, CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        if (obj.FileChunks.Count == 0) throw new InvalidOperationException("Object has no chunks associated with it.");

        return GetObjectStreamAsync(bucket, obj, 0, obj.ContentLength - 1, ct);
    }

    public async Task DeleteObjectVersionAsync(Bucket bucket, Object obj, long versionId, CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));

        var channelId = bucket.ChannelId.Value;

        var trackedObject = _db.Objects.Local.FirstOrDefault(o => o.Id == obj.Id)
                             ?? await _db.Objects.FirstOrDefaultAsync(o => o.Id == obj.Id, ct);
        if (trackedObject is null) return;

        var objectChunks = await _db.ObjectChunks
            .Where(c => c.ObjectId == trackedObject.Id)
            .ToListAsync(ct);

        if (objectChunks.Count == 0)
            throw new InvalidOperationException("Object has no chunks associated with it.");

        var messageIds = objectChunks.Select(c => c.MessageId).Distinct().ToArray();

        var storedBytes = trackedObject.StorageContentLength > 0
            ? trackedObject.StorageContentLength
            : trackedObject.ContentLength;

        _db.Objects.Remove(trackedObject);
        await _db.SaveChangesAsync(ct);

        _jobs.Create<DiscordMultiplexer>(dc => dc.BulkDeleteAsync(messageIds, channelId, ct), new EnqueuedState());

        _metrics.TrackObjectDeleted(storedBytes);
    }

    public async Task<Stream> GetObjectStreamAsync(Bucket bucket, Object obj, long startByte, long endByte,
        CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        if (obj.FileChunks.Count == 0) throw new InvalidOperationException("Object has no chunks associated with it.");

        var stream = new MemoryStream();
        var expectedLength = Math.Max(0, endByte - startByte + 1);
        if (expectedLength > 0) {
            _metrics.RecordEgress(expectedLength);
        }
        foreach (var chunk in obj.FileChunks) {
            var adjustedStart = Math.Max(startByte, chunk.StartByte) - chunk.StartByte;
            var adjustedEnd = Math.Min(endByte, chunk.EndByte) - chunk.StartByte;

            await StreamChunk(chunk.BlobUrl, adjustedStart, adjustedEnd, stream, ct);
        }

        stream.Seek(0, SeekOrigin.Begin);
        return stream;
    }


    private async Task StreamChunk(string url, long startByte, long endByte, Stream outputStream,
        CancellationToken ct) {
        var attempt = 0;
        var initialLength = outputStream.Length;

        while (true) {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Range = new RangeHeaderValue(startByte, endByte);
                _metrics.RecordDiscordRequest();
                using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
                response.EnsureSuccessStatusCode();

                await using var contentStream = await response.Content.ReadAsStreamAsync(ct);
                outputStream.Position = initialLength;
                await contentStream.CopyToAsync(outputStream, ct);
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex) && attempt <= RetryDelays.Length + 1) {
                outputStream.SetLength(initialLength);
                outputStream.Position = initialLength;
                _logger.LogWarning(ex, "Failed to stream chunk from {Url} on attempt {Attempt}", url, attempt);
                if (attempt > RetryDelays.Length) throw;
                await DelayWithBackoffAsync(attempt, ct);
            }
        }
    }


    private async Task<List<ObjectChunk>> UploadToDiscordAsync(string filePath, Bucket bucket, Object obj, long storageLength,
        CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        var channel = await ExecuteWithRetryAsync(
            () => _dcMultiplexer.GetChannelAsync((ulong)bucket.ChannelId),
            "retrieve Discord channel",
            ct);
        ArgumentNullException.ThrowIfNull(channel, nameof(channel));

        return await UploadChunksInGroupsAsync(filePath, channel, obj, storageLength, ct);
    }

    private async Task<List<ObjectChunk>> UploadChunksInGroupsAsync(string filePath, IMessageChannel channel,
        Object obj, long storageLength, CancellationToken ct) {
        var chunks = CalculateChunks(storageLength);
        var fileChunks = new List<ObjectChunk>(chunks.Count);

        for (var i = 0; i < chunks.Count; i += MaxFilesPerMessage) {
            var chunkGroup = chunks.GetRange(i, Math.Min(MaxFilesPerMessage, chunks.Count - i));
            var msg = await UploadChunksInGroupsAsync(filePath, channel, obj, chunkGroup, ct);

            var attachments = msg.Attachments.ToArray();

            for (var j = 0; j < attachments.Length; j++) {
                var attachment = attachments[j];
                var chunkInfo = chunkGroup[j];
                fileChunks.Add(new ObjectChunk {
                    ObjectId = obj.Id,

                    MessageId = msg.Id,
                    AttachmentId = attachment.Id,
                    BlobUrl = attachment.Url,

                    StartByte = chunkInfo.Offset,
                    EndByte = chunkInfo.Offset + chunkInfo.Size - 1,
                    Size = chunkInfo.Size,
                    ExpireAt = _timeProvider.GetUtcNow() + ObjectChunk.ExpireAfter,
                });
            }
        }

        return fileChunks;
    }

    private async Task<IUserMessage> UploadChunksInGroupsAsync(string filePath, IMessageChannel channel,
        Object obj, List<ChunkInfo> chunks, CancellationToken ct) {
        var attachments = chunks.Select(c =>
            new FileAttachment(new ChunkStream(filePath, c.Offset, c.Size),
                GetChunkFileName(obj, c.Index))).ToArray();
        try {
            return await ExecuteWithRetryAsync(
                async () => {
                    _metrics.RecordDiscordRequest();
                    return await channel.SendFilesAsync(attachments, options: new RequestOptions {
                        CancelToken = ct,
                        RetryMode = RetryMode.AlwaysRetry
                    });
                },
                "upload chunk group",
                ct);
        }
        finally {
            foreach (var chunk in attachments) chunk.Dispose();
        }
    }

    private static string GetChunkFileName(Object obj, int index) {
        return $"{obj.Key}.part{index}";
    }

    private List<ChunkInfo> CalculateChunks(long fileSize) {
        var chunks = new List<ChunkInfo>((int)((fileSize + MaxChunkSize - 1) / MaxChunkSize));
        long offset = 0;
        var chunkIndex = 0;

        while (offset < fileSize) {
            var chunkSize = Math.Min(MaxChunkSize, fileSize - offset);
            chunks.Add(new ChunkInfo {
                Index = chunkIndex,
                Offset = offset,
                Size = chunkSize
            });

            offset += chunkSize;
            chunkIndex++;
        }

        return chunks;
    }


    private readonly struct ChunkInfo {
        public int Index { get; init; }
        public long Offset { get; init; }
        public long Size { get; init; }
    }

    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> action, string operation, CancellationToken ct) {
        Exception? last = null;
        for (var attempt = 1; attempt <= RetryDelays.Length + 1; attempt++) {
            ct.ThrowIfCancellationRequested();
            try {
                return await action();
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex) && attempt <= RetryDelays.Length + 1) {
                last = ex;
                _logger.LogWarning(ex, "Transient failure while attempting to {Operation} (attempt {Attempt})", operation, attempt);
                if (attempt > RetryDelays.Length) break;
                await DelayWithBackoffAsync(attempt, ct);
            }
        }

        throw new InvalidOperationException($"Failed to {operation} after {RetryDelays.Length + 1} attempts", last);
    }

    private static bool IsTransient(Exception ex) => ex switch {
        HttpRequestException => true,
        TimeoutException => true,
        TaskCanceledException => true,
    HttpException httpEx when (int)httpEx.HttpCode >= 500 || httpEx.HttpCode == HttpStatusCode.TooManyRequests => true,
        _ => false
    };

    private static Task DelayWithBackoffAsync(int attempt, CancellationToken ct) {
        if (attempt - 1 >= RetryDelays.Length) return Task.CompletedTask;
        var delay = RetryDelays[attempt - 1];
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(50, 250));
        return Task.Delay(delay + jitter, ct);
    }
}