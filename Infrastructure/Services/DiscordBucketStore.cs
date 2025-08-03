using System.Net.Http.Headers;
using System.Security.Cryptography;
using Discord;
using Hangfire;

namespace Infrastructure.Services;

public class DiscordBucketStore : IBucketStore {
    private const long MaxChunkSize = 10 * 1024 * 1024; // 10 MB
    private const int MaxFilesPerMessage = 10;

    private readonly TimeProvider _timeProvider;
    private readonly IDbContext _db;
    private readonly DiscordMultiplexer _dcMultiplexer;
    private readonly HttpClient _httpClient;
    private readonly IRecurringJobManagerV2 _recurringJobs;

    public DiscordBucketStore(TimeProvider timeProvider, IDbContext db, DiscordMultiplexer dcMultiplexer,
        IHttpClientFactory httpClientFactory, IRecurringJobManagerV2 recurringJobs) {
        _timeProvider = timeProvider;
        _db = db;
        _dcMultiplexer = dcMultiplexer;
        _recurringJobs = recurringJobs;
        _httpClient = httpClientFactory.CreateClient(Constants.HttpClients.Discord);
    }

    public async Task<bool> AddObjectAsync(string filePath, Bucket bucket, Object obj, CancellationToken ct) {
        ArgumentNullException.ThrowIfNull(obj);
        var chunks = await UploadToDiscordAsync(filePath, bucket, obj);

        await using (FileStream fs = new FileStream(filePath, FileMode.Open))
            obj.Md5 = Convert.ToHexString(await MD5.HashDataAsync(fs, ct));


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
        return true;
    }

    public Task<Stream> GetObjectStreamAsync(Bucket bucket, Object obj, CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        if (obj.FileChunks.Count == 0) throw new InvalidOperationException("Object has no chunks associated with it.");
        
        return GetObjectStreamAsync(bucket, obj, 0, obj.ContentLength - 1, ct);
    }
    
    public async Task<Stream> GetObjectStreamAsync(Bucket bucket, Object obj, long startByte, long endByte, CancellationToken ct) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        if (obj.FileChunks.Count == 0) throw new InvalidOperationException("Object has no chunks associated with it.");
        
        var stream = new MemoryStream();
        foreach (var chunk in obj.FileChunks) {
            var adjustedStart = Math.Max(startByte, chunk.StartByte) - chunk.StartByte;
            var adjustedEnd = Math.Min(endByte, chunk.EndByte) - chunk.StartByte;

            await StreamChunk(chunk.BlobUrl, adjustedStart, adjustedEnd, stream, ct);
        }
        
        stream.Seek(0, SeekOrigin.Begin);
        return stream;
    }


    private async Task StreamChunk(string url, long startByte, long endByte, Stream outputStream, CancellationToken ct) {
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Range = new RangeHeaderValue(startByte, endByte);
        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        var contentStream = await response.Content.ReadAsStreamAsync(ct);
        await contentStream.CopyToAsync(outputStream, ct);
    }


    private async Task<List<ObjectChunk>> UploadToDiscordAsync(string filePath, Bucket bucket, Object obj) {
        if (!bucket.ChannelId.HasValue) throw new ArgumentNullException(nameof(bucket.ChannelId));
        var channel = await _dcMultiplexer.GetChannelAsync((ulong)bucket.ChannelId);
        ArgumentNullException.ThrowIfNull(channel, nameof(channel));

        return await UploadChunksInGroupsAsync(filePath, channel, obj);
    }

    private async Task<List<ObjectChunk>> UploadChunksInGroupsAsync(string filePath, IMessageChannel channel,
        Object obj) {
        var chunks = CalculateChunks(obj.ContentLength);
        var fileChunks = new List<ObjectChunk>(chunks.Count);

        for (var i = 0; i < chunks.Count; i += MaxFilesPerMessage) {
            var chunkGroup = chunks.GetRange(i, Math.Min(MaxFilesPerMessage, chunks.Count - i));
            var msg = await UploadChunksInGroupsAsync(filePath, channel, obj, chunkGroup);

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
        Object obj, List<ChunkInfo> chunks) {
        var attachments = chunks.Select(c =>
            new FileAttachment(new ChunkStream(filePath, c.Offset, c.Size),
                GetChunkFileName(obj, c.Index))).ToArray();
        try {
            return await channel.SendFilesAsync(attachments);
        }
        finally {
            foreach (var chunk in attachments) {
                chunk.Dispose();
            }
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
}