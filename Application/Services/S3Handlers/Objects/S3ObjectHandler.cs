using System.Collections.Specialized;
using System.Security.Cryptography;
using S3ServerLibrary.S3Objects;
using WatsonWebserver.Core;

namespace Application.Services.S3Handlers.Objects;

public class S3ObjectHandler : IS3ObjectHandler {
    private readonly ILogger<S3ObjectHandler> _logger;
    private readonly IDbContext _db;
    private readonly IBucketStore _bucketStore;
    private readonly TimeProvider _timeProvider;
    private readonly IObjectEncryptionService _encryptionService;

    public S3ObjectHandler(ILogger<S3ObjectHandler> logger, IDbContext db, IBucketStore bucketStore,
        TimeProvider timeProvider, IObjectEncryptionService encryptionService) {
        _logger = logger;
        _db = db;
        _bucketStore = bucketStore;
        _timeProvider = timeProvider;
        _encryptionService = encryptionService;
    }

    public async Task<S3Object> Read(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object read");
        EnsureAuthorized(md, ctx, "object read");
        var bucket = RequireBucket(md, ctx, "object read");

        long versionId = 1;
        if (!string.IsNullOrEmpty(ctx.Request.VersionId))
            if (!long.TryParse(ctx.Request.VersionId, out versionId))
                throw new S3Exception(new Error(ErrorCode.NoSuchVersion));

        if (md.Obj is null) {
            if (versionId is 1) {
                _logger.LogWarning("Object not found for bucket {Bucket} and key {Key}", ctx.Request.Bucket,
                    ctx.Request.Key);
                throw new S3Exception(new Error(ErrorCode.NoSuchKey));
            }

            _logger.LogWarning(
                "Object version {VersionId} not found for bucket {Bucket} and key {Key}", versionId,
                ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        var isLatest = true;
        var latestVersion = (await _db.Objects.ReadObjectLatestMetadataAsync(bucket.Id, md.Obj.Key))?.Version;
        if (md.Obj.Version < latestVersion) isLatest = false;

        var user = await _db.Users.ReadUserByIdAsync(md.Obj.OwnerId);
        var owner = user is not null ? new Owner(user.Id, user.Name) : null;

        var stream = await _bucketStore.GetObjectStreamAsync(bucket, md.Obj, ctx.Http.Token);

        if (md.Obj.IsEncrypted) {
            try {
                stream = await _encryptionService.DecryptAsync(stream, md.Obj, ctx.Http.Token);
                ctx.Response.Headers[Constants.Headers.ServerSideEncryption] =
                    md.Obj.EncryptionAlgorithm ?? Constants.EncryptionAlgorithms.Aes256;
            }
            catch (Exception ex) {
                _logger.LogError(ex, "Failed to decrypt object {Bucket}/{Key} version {Version}",
                    ctx.Request.Bucket, ctx.Request.Key, md.Obj.Version);
                throw new S3Exception(new Error(ErrorCode.InternalError));
            }
        }

        return new S3Object(md.Obj.Key, md.Obj.Version.ToString(), isLatest, md.Obj.LastUpdateUtc.UtcDateTime,
            md.Obj.Etag,
            md.Obj.ContentLength, owner, stream, md.Obj.ContentType);
    }


    public async Task Write(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object write");
        EnsureAuthorized(md, ctx, "object write");
        var bucket = RequireBucket(md, ctx, "object write");

        _logger.LogInformation(
            "Write handler dispatched for {Method} {Uri} (request type {RequestType}, uploadId {UploadId}, partNumber {PartNumber})",
            ctx.Http.Request.Method, ctx.Http.Request.Url.Full, ctx.Request.RequestType, ctx.Request.UploadId,
            ctx.Request.PartNumber);

        var existingObject =
            await _db.Objects.ReadObjectLatestMetadataAsync(bucket.Id, ctx.Request.Key, ctx.Http.Token);
        var replaceCurrent = existingObject is not null && !bucket.EnableVersioning;

        #region Populate-Metadata

        void PopulateUserMetadata(User? user, Object target) {
            if (user is not null) {
                target.AuthorId = user.Id;
                target.OwnerId = user.Id;
            }
            else {
                var source = ctx.Http.Request.Source;
                var fallback = $"{source.IpAddress}:{source.Port}";
                target.AuthorId = fallback;
                target.OwnerId = fallback;
            }
        }

        var obj = new Object();

        PopulateUserMetadata(md.User, obj);

        obj.BucketId = bucket.Id;
        obj.Version = existingObject is null ? 1 : existingObject.Version + 1;
        obj.BlobFilename = obj.Id;
        obj.ContentLength = ctx.Http.Request.ContentLength;
        obj.ContentType = ctx.Http.Request.ContentType;
        obj.DeleteMarker = false;
        obj.ExpirationUtc = null;
        obj.Key = ctx.Request.Key;

        if (obj.ContentLength == 0 && obj.Key.EndsWith('/')) obj.IsFolder = true;

        #endregion

        var encryptionRequest = ResolveEncryptionRequest(ctx.Http.Request.Headers);
        var copySourceHeader = ctx.Http.Request.Headers["x-amz-copy-source"];
        if (!string.IsNullOrWhiteSpace(copySourceHeader)) {
            await HandleCopyAsync(ctx, md, obj, copySourceHeader, encryptionRequest);
            return;
        }

        #region Write-Data-to-Temp-and-to-Bucket

        var tempDirectory = Constants.File.TempDir;
        if (!Directory.Exists(tempDirectory)) {
            Directory.CreateDirectory(tempDirectory);
        }

        var tempFilePath = Path.Combine(tempDirectory, Ulid.NewUlid().ToString());
        long totalLength = 0;

        try {
            await using (FileStream fs = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write,
                             FileShare.None)) {
                if (ctx.Request.Chunked) {
                    while (true) {
                        Chunk chunk = await ctx.Request.ReadChunk();
                        if (chunk == null) break;

                        if (chunk.Data is { Length: > 0 }) {
                            await fs.WriteAsync(chunk.Data.AsMemory(0, chunk.Data.Length), ctx.Http.Token);
                            totalLength += chunk.Data.Length;
                        }

                        if (chunk.IsFinal) break;
                    }
                }
                else if (ctx.Request.Data != null && ctx.Http.Request.ContentLength > 0) {
                    var bytesRemaining = ctx.Http.Request.ContentLength;
                    var buffer = new byte[65536];

                    while (bytesRemaining > 0) {
                        var bytesRead = await ctx.Request.Data.ReadAsync(buffer, ctx.Http.Token);
                        if (bytesRead <= 0) continue;

                        bytesRemaining -= bytesRead;
                        await fs.WriteAsync(buffer.AsMemory(0, bytesRead), ctx.Http.Token);
                    }

                    totalLength = ctx.Http.Request.ContentLength;
                }
            }
        }
        catch (Exception e) {
            _logger.LogError(e, "Error writing object {Bucket}/{Key} using tempfile {TempFilePath}",
                ctx.Request.Bucket, ctx.Request.Key, tempFilePath);
            DeleteFileIfExists(tempFilePath);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        obj.ContentLength = totalLength;

        _logger.LogInformation("Persisting single-request object for {Bucket}/{Key} version {Version} with {Length} bytes (replace: {ReplaceCurrent})",
            ctx.Request.Bucket, ctx.Request.Key, obj.Version, obj.ContentLength, replaceCurrent);

        EncryptionOutcome? encryptionOutcome = null;
        try {
            encryptionOutcome = await PersistObjectAsync(ctx, md, obj, tempFilePath, encryptionRequest);
        }
        finally {
            DeleteFileIfExists(tempFilePath);
        }

        if (encryptionOutcome is not null) {
            ctx.Response.Headers[Constants.Headers.ServerSideEncryption] = encryptionOutcome.Algorithm;
        }

        #endregion

        await ApplyAclHeadersAsync(ctx, md, obj);

        if (replaceCurrent && existingObject is not null) {
            await CleanupReplacedObjectAsync(ctx, bucket, existingObject);
        }

        if (!string.IsNullOrWhiteSpace(obj.Etag)) {
            ctx.Response.Headers["ETag"] = $"\"{obj.Etag.ToLowerInvariant()}\"";
        }
    }

    private RequestMetadata RequireMetadata(S3Context ctx, string operation) {
        if (ctx.Metadata is RequestMetadata md && md is not null) return md;

        _logger.LogWarning("Request metadata is null during {Operation} for request {@Request}", operation,
            ctx.Request);
        throw new S3Exception(new Error(ErrorCode.InternalError));
    }

    private void EnsureAuthorized(RequestMetadata md, S3Context ctx, string operation) {
        if (md.Authorization != EAuthorizationResult.NotAuthorized) return;

        if (!string.IsNullOrWhiteSpace(ctx.Request.Key)) {
            _logger.LogWarning("Unauthorized {Operation} for bucket {Bucket} and key {Key}", operation,
                ctx.Request.Bucket,
                ctx.Request.Key);
        }
        else {
            _logger.LogWarning("Unauthorized {Operation} for bucket {Bucket}", operation, ctx.Request.Bucket);
        }

        throw new S3Exception(new Error(ErrorCode.AccessDenied));
    }

    private Domain.Entities.Bucket RequireBucket(RequestMetadata md, S3Context ctx, string operation) {
        if (md.Bucket is not null) return md.Bucket;

        _logger.LogWarning("Bucket not found during {Operation} {@Request}", operation, ctx.Request);
        throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
    }

    private async Task<EncryptionOutcome?> PersistObjectAsync(S3Context ctx, RequestMetadata md, Object obj,
        string plaintextFilePath,
        EncryptionRequest? encryptionRequest) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(md);
        ArgumentNullException.ThrowIfNull(obj);
        ArgumentException.ThrowIfNullOrEmpty(plaintextFilePath);

        var bucket = RequireBucket(md, ctx, "object persist");

        EncryptionOutcome? encryptionOutcome = null;
        var payloadPath = plaintextFilePath;
        var writeSuccess = false;

        try {
            _logger.LogInformation(
                "Persisting object {Bucket}/{Key} version {Version} from payload {PayloadPath} (SSE requested: {SseRequested})",
                ctx.Request.Bucket, ctx.Request.Key, obj.Version, payloadPath, encryptionRequest is not null);
            if (encryptionRequest is not null) {
                try {
                    encryptionOutcome =
                        await _encryptionService.EncryptAsync(plaintextFilePath, encryptionRequest, ctx.Http.Token);
                    payloadPath = encryptionOutcome.EncryptedFilePath;
                    ApplyEncryptionOutcome(obj, encryptionOutcome, encryptionRequest);
                }
                catch (Exception ex) {
                    _logger.LogError(ex, "Failed to encrypt object {Bucket}/{Key}", ctx.Request.Bucket,
                        ctx.Request.Key);
                    throw new S3Exception(new Error(ErrorCode.InternalError));
                }
            }
            else {
                PrepareUnencryptedObject(obj);
            }

            writeSuccess = await _bucketStore.AddObjectAsync(payloadPath, bucket, obj, ctx.Http.Token);
        }
        catch (S3Exception) {
            throw;
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to persist object {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }
        finally {
            if (encryptionOutcome is not null) {
                DeleteFileIfExists(encryptionOutcome.EncryptedFilePath);
            }
        }

        if (!writeSuccess) {
            _logger.LogWarning("Bucket store rejected object {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        _logger.LogInformation(
            "Bucket store accepted object {Bucket}/{Key} version {Version} (content length {Length}, storage length {StorageLength})",
            ctx.Request.Bucket, ctx.Request.Key, obj.Version, obj.ContentLength, obj.StorageContentLength);

        return encryptionOutcome;
    }

    private async Task HandleCopyAsync(S3Context ctx, RequestMetadata md, Object obj, string copySourceHeader,
        EncryptionRequest? encryptionRequest) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(md);
        ArgumentNullException.ThrowIfNull(obj);
        ArgumentException.ThrowIfNullOrEmpty(copySourceHeader);

        RequireBucket(md, ctx, "object copy target");

        var trimmed = copySourceHeader.Trim();
        if (trimmed.StartsWith('/')) trimmed = trimmed[1..];
        var separatorIndex = trimmed.IndexOf('/');
        if (separatorIndex <= 0 || separatorIndex == trimmed.Length - 1) {
            _logger.LogWarning("Invalid copy source header {Header}", copySourceHeader);
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));
        }

        var sourceBucketName = System.Uri.UnescapeDataString(trimmed[..separatorIndex]);
        var sourceKeySegment = trimmed[(separatorIndex + 1)..];
        string? versionIdQuery = null;
        var queryIndex = sourceKeySegment.IndexOf('?');
        if (queryIndex >= 0) {
            versionIdQuery = sourceKeySegment[(queryIndex + 1)..];
            sourceKeySegment = sourceKeySegment[..queryIndex];
        }

        var sourceKey = System.Uri.UnescapeDataString(sourceKeySegment);

        var sourceBucket = await _db.Buckets
            .AsNoTracking()
            .FirstOrDefaultAsync(b => b.Name == sourceBucketName, ctx.Http.Token);
        if (sourceBucket is null) {
            _logger.LogWarning("Copy source bucket {Bucket} not found", sourceBucketName);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        long? versionId = null;
        var versionHeader = ctx.Http.Request.Headers["x-amz-copy-source-version-id"];
        if (!string.IsNullOrWhiteSpace(versionHeader)) {
            if (!long.TryParse(versionHeader, out var parsedVersion)) {
                throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
            }

            versionId = parsedVersion;
        }
        else if (!string.IsNullOrWhiteSpace(versionIdQuery)) {
            var segments = versionIdQuery.Split('&', System.StringSplitOptions.RemoveEmptyEntries);
            foreach (var segment in segments) {
                var parts = segment.Split('=', 2);
                if (parts.Length == 2 && parts[0].Equals("versionId", System.StringComparison.OrdinalIgnoreCase)) {
                    var value = System.Uri.UnescapeDataString(parts[1]);
                    if (!long.TryParse(value, out var parsedVersion)) {
                        throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
                    }

                    versionId = parsedVersion;
                    break;
                }
            }
        }

        IQueryable<Object> objectQuery = _db.Objects
            .AsNoTracking()
            .Include(o => o.FileChunks)
            .Where(o => o.BucketId == sourceBucket.Id && o.Key == sourceKey);

        Object? sourceObject;
        if (versionId.HasValue) {
            sourceObject = await objectQuery.FirstOrDefaultAsync(o => o.Version == versionId.Value, ctx.Http.Token);
            if (sourceObject is null) {
                _logger.LogWarning("Copy source version {Version} not found for {Bucket}/{Key}", versionId.Value,
                    sourceBucketName, sourceKey);
                throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
            }
        }
        else {
            sourceObject = await objectQuery
                .OrderByDescending(o => o.Version)
                .FirstOrDefaultAsync(ctx.Http.Token);
            if (sourceObject is null) {
                _logger.LogWarning("Copy source object {Bucket}/{Key} not found", sourceBucketName, sourceKey);
                throw new S3Exception(new Error(ErrorCode.NoSuchKey));
            }
        }

        if (sourceObject.DeleteMarker) {
            _logger.LogWarning("Copy source object {Bucket}/{Key} is a delete marker", sourceBucketName, sourceKey);
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        obj.ContentType = sourceObject.ContentType;
        obj.IsFolder = sourceObject.IsFolder;

        var tempDirectory = Constants.File.TempDir;
        if (!Directory.Exists(tempDirectory)) {
            Directory.CreateDirectory(tempDirectory);
        }

        var tempPlaintextPath = Path.Combine(tempDirectory, Ulid.NewUlid().ToString());

        try {
            await using var sourceStream =
                await _bucketStore.GetObjectStreamAsync(sourceBucket, sourceObject, ctx.Http.Token);
            Stream payloadStream = sourceStream;
            Stream? decryptedStream = null;

            try {
                if (sourceObject.IsEncrypted) {
                    decryptedStream = await _encryptionService.DecryptAsync(sourceStream, sourceObject, ctx.Http.Token);
                    payloadStream = decryptedStream;
                }

                await using var fs = new FileStream(tempPlaintextPath, FileMode.Create, FileAccess.Write,
                    FileShare.None);
                await payloadStream.CopyToAsync(fs, ctx.Http.Token);
            }
            finally {
                decryptedStream?.Dispose();
            }
        }
        catch (S3Exception) {
            DeleteFileIfExists(tempPlaintextPath);
            throw;
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to hydrate copy source {Bucket}/{Key}", sourceBucketName, sourceKey);
            DeleteFileIfExists(tempPlaintextPath);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        try {
            var contentInfo = new FileInfo(tempPlaintextPath);
            obj.ContentLength = contentInfo.Length;

            var encryptionOutcome = await PersistObjectAsync(ctx, md, obj, tempPlaintextPath, encryptionRequest);

            if (encryptionOutcome is not null) {
                ctx.Response.Headers[Constants.Headers.ServerSideEncryption] = encryptionOutcome.Algorithm;
            }

            await ApplyAclHeadersAsync(ctx, md, obj);

            if (!string.IsNullOrWhiteSpace(obj.Etag)) {
                ctx.Response.Headers["ETag"] = $"\"{obj.Etag.ToLowerInvariant()}\"";
            }
        }
        finally {
            DeleteFileIfExists(tempPlaintextPath);
        }
    }

    public async Task Delete(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object delete");
        EnsureAuthorized(md, ctx, "object delete");
        var bucket = RequireBucket(md, ctx, "object delete");

        long versionId = 1;
        if (!string.IsNullOrEmpty(ctx.Request.VersionId)) {
            if (!long.TryParse(ctx.Request.VersionId, out versionId)) {
                throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
            }
        }

        if (md.Obj is null) {
            if (versionId == 1) {
                _logger.LogInformation("Delete accepted for missing object {Bucket}/{Key}", ctx.Request.Bucket,
                    ctx.Request.Key);
                return;
            }

            _logger.LogWarning("Object version {VersionId} not found for bucket {Bucket} and key {Key}", versionId,
                ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        if (bucket.EnableVersioning) {
            md.Obj.DeleteMarker = true;
            _db.Objects.Update(md.Obj);
            await _db.SaveChangesAsync();

            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            return;
        }

        // Delete object version, ACL, and tags using _bucketStore and _db
        await _bucketStore.DeleteObjectVersionAsync(bucket, md.Obj, versionId, ctx.Http.Token);
        await _db.ObjectAcls.DeleteObjectVersionAclAsync(bucket.Id, md.Obj.Id, versionId);
        await _db.ObjectTags.DeleteObjectVersionTagsAsync(bucket.Id, md.Obj.Id, versionId);
    }

    public async Task<Tagging> ReadTags(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object tag read");
        EnsureAuthorized(md, ctx, "object tag read");
        RequireBucket(md, ctx, "object tag read");

        if (md.Obj is null) {
            _logger.LogWarning("Object not found for tag read {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        var cancellation = ctx.Http.Token;

        var objectTags = await _db.ObjectTags
            .AsNoTracking()
            .Where(t => t.ObjectId == md.Obj.Id)
            .OrderBy(t => t.Key)
            .ToListAsync(cancellation);

        var tags = objectTags.Count == 0
            ? new List<Tag>()
            : objectTags.Select(t => new Tag(t.Key, t.Value)).ToList();

        return new Tagging(new TagSet(tags));
    }

    public async Task WriteTags(S3Context ctx, Tagging tagging) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object tag write");
        EnsureAuthorized(md, ctx, "object tag write");
        var bucket = RequireBucket(md, ctx, "object tag write");

        if (md.Obj is null) {
            _logger.LogWarning("Object not found for tag write {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        var cancellation = ctx.Http.Token;

        var existing = await _db.ObjectTags
            .AsTracking()
            .Where(t => t.ObjectId == md.Obj.Id)
            .ToListAsync(cancellation);
        if (existing.Count > 0) _db.ObjectTags.RemoveRange(existing);

        var incomingTags = tagging?.Tags?.Tags ?? new List<Tag>();
        var uniqueTags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var tag in incomingTags) {
            if (tag is null) continue;
            if (string.IsNullOrWhiteSpace(tag.Key)) {
                _logger.LogWarning("Rejecting object tag write with empty key for {Bucket}/{Key}", ctx.Request.Bucket,
                    ctx.Request.Key);
                throw new S3Exception(new Error(ErrorCode.InvalidArgument));
            }

            uniqueTags[tag.Key] = tag.Value ?? string.Empty;
        }

        if (uniqueTags.Count > 0) {
            var now = _timeProvider.GetUtcNow();
            foreach (var kvp in uniqueTags) {
                _db.ObjectTags.Add(new ObjectTag {
                    BucketId = bucket.Id,
                    ObjectId = md.Obj.Id,
                    Key = kvp.Key,
                    Value = kvp.Value,
                    CreatedUtc = now
                });
            }
        }

        await _db.SaveChangesAsync(cancellation);
    }

    public async Task DeleteTags(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "object tag delete");
        EnsureAuthorized(md, ctx, "object tag delete");
        RequireBucket(md, ctx, "object tag delete");

        if (md.Obj is null) {
            _logger.LogWarning("Object not found for tag delete {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        var cancellation = ctx.Http.Token;

        var existing = await _db.ObjectTags
            .AsTracking()
            .Where(t => t.ObjectId == md.Obj.Id)
            .ToListAsync(cancellation);

        if (existing.Count == 0) return;

        _db.ObjectTags.RemoveRange(existing);
        await _db.SaveChangesAsync(cancellation);
    }

    private async Task ApplyAclHeadersAsync(S3Context ctx, RequestMetadata md, Object obj) {
        if (md.User is null) return;
        var bucket = RequireBucket(md, ctx, "ACL apply");

        List<Grant> grants = await Grants.GrantsFromHeaders(_db, md.User, ctx.Http.Request.Headers);
        if (grants is not { Count: > 0 }) return;

        foreach (Grant curr in grants) {
            if (curr.Grantee is null) continue;

            var permitRead = false;
            var permitWrite = false;
            var permitReadAcp = false;
            var permitWriteAcp = false;
            var fullControl = false;

            switch (curr.Permission) {
                case PermissionEnum.Read:
                    permitRead = true;
                    break;
                case PermissionEnum.Write:
                    permitWrite = true;
                    break;
                case PermissionEnum.ReadAcp:
                    permitReadAcp = true;
                    break;
                case PermissionEnum.WriteAcp:
                    permitWriteAcp = true;
                    break;
                case PermissionEnum.FullControl:
                    fullControl = true;
                    break;
            }

            if (!string.IsNullOrEmpty(curr.Grantee.ID)) {
                var targetUser = await _db.Users.ReadUserByIdAsync(curr.Grantee.ID);
                if (targetUser is null) {
                    _logger.LogWarning(
                        "Unable to retrieve user {UserId} to add ACL to object {Bucket}/{Key} version {Version}",
                        curr.Grantee.ID, ctx.Request.Bucket, ctx.Request.Key, obj.Version);
                    continue;
                }

                var objectAcl = new ObjectAcl {
                    UserId = curr.Grantee.ID,
                    IssuedByUserId = md.User.Id,
                    BucketId = bucket.Id,
                    ObjectId = obj.Id,
                    PermitRead = permitRead,
                    PermitWrite = permitWrite,
                    PermitReadAcp = permitReadAcp,
                    PermitWriteAcp = permitWriteAcp,
                    FullControl = fullControl,
                    CreatedUtc = _timeProvider.GetUtcNow(),
                    UserGroup = null!
                };

                _db.ObjectAcls.Add(objectAcl);
            }
            else if (!string.IsNullOrEmpty(curr.Grantee.URI)) {
                var objectAcl = new ObjectAcl {
                    UserGroup = curr.Grantee.URI,
                    IssuedByUserId = md.User.Id,
                    BucketId = bucket.Id,
                    ObjectId = obj.Id,
                    PermitRead = permitRead,
                    PermitWrite = permitWrite,
                    PermitReadAcp = permitReadAcp,
                    PermitWriteAcp = permitWriteAcp,
                    FullControl = fullControl,
                    CreatedUtc = _timeProvider.GetUtcNow(),
                    UserId = null!
                };

                _db.ObjectAcls.Add(objectAcl);
            }
        }

        await _db.SaveChangesAsync(ctx.Http.Token);
    }

    private async Task CleanupReplacedObjectAsync(S3Context ctx, Domain.Entities.Bucket bucket, Object previous) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(bucket);
        ArgumentNullException.ThrowIfNull(previous);

        try {
            await _db.ObjectAcls.DeleteObjectVersionAclAsync(bucket.Id, previous.Id, previous.Version, ctx.Http.Token);
            await _db.ObjectTags.DeleteObjectVersionTagsAsync(bucket.Id, previous.Id, previous.Version, ctx.Http.Token);
            await _bucketStore.DeleteObjectVersionAsync(bucket, previous, previous.Version, ctx.Http.Token);
        }
        catch (S3Exception) {
            throw;
        }
        catch (Exception ex) {
            _logger.LogError(ex,
                "Failed to cleanup previous object version {Bucket}/{Key} version {Version}",
                ctx.Request.Bucket, ctx.Request.Key, previous.Version);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }
    }

    private EncryptionRequest? ResolveEncryptionRequest(NameValueCollection headers) {
        if (headers is null || headers.Count == 0) return null;

        if (!string.IsNullOrWhiteSpace(headers[Constants.Headers.ServerSideEncryptionCustomerAlgorithm]) ||
            !string.IsNullOrWhiteSpace(headers[Constants.Headers.ServerSideEncryptionCustomerKey]) ||
            !string.IsNullOrWhiteSpace(headers[Constants.Headers.ServerSideEncryptionCustomerKeyMd5])) {
            _logger.LogWarning("Received unsupported SSE-C headers");
            throw new S3Exception(new Error(ErrorCode.NotImplemented));
        }

        if (!string.IsNullOrWhiteSpace(headers[Constants.Headers.ServerSideEncryptionAwsKmsKeyId])) {
            _logger.LogWarning("Received unsupported SSE-KMS request");
            throw new S3Exception(new Error(ErrorCode.NotImplemented));
        }

        var algorithm = headers[Constants.Headers.ServerSideEncryption];
        if (string.IsNullOrWhiteSpace(algorithm)) return null;

        if (!string.Equals(algorithm, Constants.EncryptionAlgorithms.Aes256,
                System.StringComparison.OrdinalIgnoreCase)) {
            _logger.LogWarning("Unsupported SSE algorithm {Algorithm} requested", algorithm);
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));
        }

        if (!string.IsNullOrWhiteSpace(headers[Constants.Headers.ServerSideEncryptionContext])) {
            _logger.LogWarning("Encryption context supplied for SSE-S3 request; rejecting");
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));
        }

        return new EncryptionRequest(Constants.EncryptionAlgorithms.Aes256, null, null);
    }

    private static void ApplyEncryptionOutcome(Object obj, EncryptionOutcome outcome, EncryptionRequest request) {
        obj.IsEncrypted = true;
        obj.EncryptionAlgorithm = outcome.Algorithm;
        obj.EncryptionKeyId = outcome.KeyId;
        obj.EncryptionMetadata = outcome.MetadataJson;
        obj.EncryptedDataKey = outcome.ProtectedDataKey;
        obj.EncryptionContext = request.Context;
        obj.StorageContentLength = outcome.CiphertextLength;
        obj.Md5 = outcome.PlaintextMd5Hex;
        obj.Etag = outcome.PlaintextMd5Hex;
    }

    private static void PrepareUnencryptedObject(Object obj) {
        obj.IsEncrypted = false;
        obj.EncryptionAlgorithm = null;
        obj.EncryptionKeyId = null;
        obj.EncryptionMetadata = null;
        obj.EncryptedDataKey = null;
        obj.EncryptionContext = null;
        obj.StorageContentLength = obj.ContentLength;
        obj.Md5 = string.Empty;
        obj.Etag = string.Empty;
    }

    private EncryptionRequest? BuildEncryptionRequest(MultipartUpload upload) {
        ArgumentNullException.ThrowIfNull(upload);
        if (!upload.UseServerSideEncryption) return null;

        var algorithm = upload.EncryptionAlgorithm;
        if (string.IsNullOrWhiteSpace(algorithm)) algorithm = Constants.EncryptionAlgorithms.Aes256;

        if (!string.Equals(algorithm, Constants.EncryptionAlgorithms.Aes256,
                System.StringComparison.OrdinalIgnoreCase)) {
            _logger.LogWarning("Unsupported multipart SSE algorithm {Algorithm} requested", algorithm);
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));
        }

        return new EncryptionRequest(Constants.EncryptionAlgorithms.Aes256, upload.EncryptionKeyId,
            upload.EncryptionContext);
    }

    private (string Id, string DisplayName) ResolveActor(RequestMetadata md, S3Context ctx,
        MultipartUpload? upload = null) {
        if (md.User is not null) return (md.User.Id, md.User.Name);

        if (upload is not null) return (upload.OwnerId, upload.OwnerDisplayName);

        var source = ctx.Http.Request.Source;
        var fallback = $"{source.IpAddress}:{source.Port}";
        return (fallback, fallback);
    }

    private async Task<MultipartUpload> GetMultipartUploadAsync(S3Context ctx, RequestMetadata md) {
        var bucket = RequireBucket(md, ctx, "multipart lookup");

        if (string.IsNullOrEmpty(ctx.Request.UploadId)) {
            _logger.LogWarning("UploadId missing for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchUpload));
        }

        var upload =
            await _db.MultipartUploads.ReadMultipartUploadAsync(bucket.Id, ctx.Request.Key, ctx.Request.UploadId,
                ctx.Http.Token);
        if (upload is null || upload.IsAborted) {
            _logger.LogWarning("Multipart upload {UploadId} not found for bucket {Bucket} and key {Key}",
                ctx.Request.UploadId, ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchUpload));
        }

        EnsureUploadDirectory(upload);
        return upload;
    }

    private static void EnsureUploadDirectory(MultipartUpload upload) {
        if (!Directory.Exists(upload.UploadDirectory)) {
            Directory.CreateDirectory(upload.UploadDirectory);
        }
    }

    private static string GetPartFilePath(MultipartUpload upload, int partNumber) {
        return Path.Combine(upload.UploadDirectory, $"{partNumber:D5}.part");
    }

    private static string NormalizeEtag(string? etag) {
        if (string.IsNullOrWhiteSpace(etag)) return string.Empty;

        etag = etag.Trim();
        if (etag.StartsWith('"') && etag.EndsWith('"') && etag.Length >= 2)
            etag = etag.Substring(1, etag.Length - 2);

        return etag.ToUpperInvariant();
    }

    private static void DeleteFileIfExists(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return;

        try {
            if (File.Exists(path)) File.Delete(path);
        }
        catch {
            // ignored
        }
    }

    private static void SafeDeleteDirectory(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return;

        try {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
        catch {
            // ignored
        }
    }

    public async Task<InitiateMultipartUploadResult> CreateMultipartUpload(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "multipart initiate");
        EnsureAuthorized(md, ctx, "multipart initiate");
        var bucket = RequireBucket(md, ctx, "multipart initiate");

        var encryptionRequest = ResolveEncryptionRequest(ctx.Http.Request.Headers);
        var (ownerId, ownerName) = ResolveActor(md, ctx);
        var now = _timeProvider.GetUtcNow();

        var upload = new MultipartUpload {
            BucketId = bucket.Id,
            Key = ctx.Request.Key,
            OwnerId = ownerId,
            OwnerDisplayName = ownerName,
            InitiatorId = ownerId,
            InitiatorDisplayName = ownerName,
            ContentType = string.IsNullOrWhiteSpace(ctx.Http.Request.ContentType)
                ? "application/octet-stream"
                : ctx.Http.Request.ContentType!,
            CreatedUtc = now,
            LastUpdatedUtc = now,
            UseServerSideEncryption = encryptionRequest is not null,
            EncryptionAlgorithm = encryptionRequest?.Algorithm,
            EncryptionKeyId = encryptionRequest?.KeyId,
            EncryptionContext = encryptionRequest?.Context
        };

        upload.UploadDirectory = Path.Combine(Constants.File.TempDir, "multipart", bucket.Id, upload.Id);
        EnsureUploadDirectory(upload);

        _db.MultipartUploads.Add(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        if (encryptionRequest is not null) {
            ctx.Response.Headers[Constants.Headers.ServerSideEncryption] = encryptionRequest.Algorithm;
        }

        _logger.LogInformation(
            "Multipart upload initiated for {Bucket}/{Key} with upload ID {UploadId}. SSE enabled: {SseEnabled}",
            ctx.Request.Bucket, ctx.Request.Key, upload.Id, encryptionRequest is not null);

        return new InitiateMultipartUploadResult(ctx.Request.Bucket, ctx.Request.Key, upload.Id);
    }

    public async Task UploadPart(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "multipart upload-part");
        EnsureAuthorized(md, ctx, "multipart upload-part");

        _logger.LogInformation(
            "UploadPart handler dispatched for {Method} {Uri} (uploadId {UploadId}, partNumber {PartNumber})",
            ctx.Http.Request.Method, ctx.Http.Request.Url.Full, ctx.Request.UploadId, ctx.Request.PartNumber);

        var upload = await GetMultipartUploadAsync(ctx, md);
        var partNumber = ctx.Request.PartNumber;
        if (partNumber <= 0) {
            _logger.LogWarning("Invalid part number {PartNumber} for upload {UploadId}", partNumber,
                ctx.Request.UploadId);
            throw new S3Exception(new Error(ErrorCode.InvalidArgument));
        }

        var cancellationToken = ctx.Http.Token;
        var partFilePath = GetPartFilePath(upload, partNumber);
        long totalLength = 0;

        try {
            await using var fs = new FileStream(partFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            if (ctx.Request.Chunked) {
                while (true) {
                    Chunk chunk = await ctx.Request.ReadChunk();
                    if (chunk == null) break;

                    if (chunk.Data is { Length: > 0 }) {
                        await fs.WriteAsync(chunk.Data.AsMemory(0, chunk.Data.Length), cancellationToken);
                        totalLength += chunk.Data.Length;
                    }

                    if (chunk.IsFinal) break;
                }
            }
            else if (ctx.Request.Data != null) {
                var buffer = new byte[65536];
                while (true) {
                    var bytesRead =
                        await ctx.Request.Data.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken);
                    if (bytesRead <= 0) break;

                    await fs.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                    totalLength += bytesRead;
                }
            }
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Error writing multipart part {Bucket}/{Key} upload {UploadId} part {PartNumber}",
                ctx.Request.Bucket, ctx.Request.Key, ctx.Request.UploadId, partNumber);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        string etag;
        try {
            await using var readStream = new FileStream(partFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            etag = Convert.ToHexString(await MD5.HashDataAsync(readStream, cancellationToken));
        }
        catch (Exception ex) {
            _logger.LogError(ex,
                "Failed to compute checksum for multipart part {Bucket}/{Key} upload {UploadId} part {PartNumber}",
                ctx.Request.Bucket, ctx.Request.Key, ctx.Request.UploadId, partNumber);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        var now = _timeProvider.GetUtcNow();
        var part = upload.Parts.FirstOrDefault(p => p.PartNumber == partNumber);
        if (part is null) {
            part = new MultipartUploadPart {
                UploadId = upload.Id,
                PartNumber = partNumber,
                Size = totalLength,
                Etag = etag,
                TempFilePath = partFilePath,
                CreatedUtc = now,
                LastUpdatedUtc = now
            };
            upload.Parts.Add(part);
            _db.MultipartUploadParts.Add(part);
        }
        else {
            part.Size = totalLength;
            part.Etag = etag;
            part.TempFilePath = partFilePath;
            part.LastUpdatedUtc = now;
        }

        upload.LastUpdatedUtc = now;
        await _db.SaveChangesAsync(cancellationToken);

        ctx.Response.Headers["ETag"] = $"\"{etag.ToLowerInvariant()}\"";

        _logger.LogInformation(
            "Stored multipart part {PartNumber} ({Length} bytes) for {Bucket}/{Key} upload {UploadId}",
            partNumber, totalLength, ctx.Request.Bucket, ctx.Request.Key, ctx.Request.UploadId);
    }

    public async Task<ListPartsResult> ReadParts(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "multipart list-parts");
        EnsureAuthorized(md, ctx, "multipart list-parts");

        var upload = await GetMultipartUploadAsync(ctx, md);

        var partNumberMarker = ctx.Request.PartNumberMarker > 0 ? ctx.Request.PartNumberMarker : 0;
        var maxParts = ctx.Request.MaxParts > 0 ? ctx.Request.MaxParts : 1000;

        var orderedParts = upload.Parts.OrderBy(p => p.PartNumber).ToList();
        var eligibleParts = orderedParts.Where(p => p.PartNumber > partNumberMarker).ToList();
        var selectedParts = eligibleParts.Take(maxParts).ToList();

        var isTruncated = selectedParts.Count < eligibleParts.Count;
        var nextMarker = isTruncated && selectedParts.Count > 0 ? selectedParts[^1].PartNumber : 0;

        var resultParts = new List<Part>(selectedParts.Count);
        foreach (var part in selectedParts) {
            if (part.Size > int.MaxValue) {
                _logger.LogWarning(
                    "Multipart part size exceeds supported range for upload {UploadId} part {PartNumber}", upload.Id,
                    part.PartNumber);
                throw new S3Exception(new Error(ErrorCode.InvalidPart));
            }

            resultParts.Add(new Part {
                PartNumber = part.PartNumber,
                Size = (int)part.Size,
                ETag = $"\"{part.Etag.ToLowerInvariant()}\"",
                LastModified = part.LastUpdatedUtc.UtcDateTime
            });
        }

        return new ListPartsResult {
            Bucket = ctx.Request.Bucket,
            Key = ctx.Request.Key,
            UploadId = upload.Id,
            PartNumberMarker = partNumberMarker,
            NextPartNumberMarker = nextMarker,
            MaxParts = maxParts,
            IsTruncated = isTruncated,
            Parts = resultParts,
            Initiator = new Owner(upload.InitiatorId, upload.InitiatorDisplayName),
            Owner = new Owner(upload.OwnerId, upload.OwnerDisplayName),
            StorageClass = StorageClassEnum.STANDARD
        };
    }

    public async Task<CompleteMultipartUploadResult> CompleteMultipartUpload(S3Context ctx,
        CompleteMultipartUpload request) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        ArgumentNullException.ThrowIfNull(request, nameof(request));
        var md = RequireMetadata(ctx, "multipart complete");
        EnsureAuthorized(md, ctx, "multipart complete");
        var bucket = RequireBucket(md, ctx, "multipart complete");

        if (request.Parts is null || request.Parts.Count == 0) {
            _logger.LogWarning("Multipart completion request missing parts for upload {UploadId}",
                ctx.Request.UploadId);
            throw new S3Exception(new Error(ErrorCode.InvalidPart));
        }

        var upload = await GetMultipartUploadAsync(ctx, md);

        var completionEncryptionRequest = ResolveEncryptionRequest(ctx.Http.Request.Headers);
        if (completionEncryptionRequest is not null && !upload.UseServerSideEncryption) {
            _logger.LogWarning(
                "Multipart completion attempted to enable SSE for upload {UploadId} without prior negotiation",
                upload.Id);
            throw new S3Exception(new Error(ErrorCode.InvalidRequest));
        }

        var orderedRequestParts = request.Parts.OrderBy(p => p.PartNumber).ToList();
        if (orderedRequestParts.Count == 0) {
            throw new S3Exception(new Error(ErrorCode.InvalidPart));
        }

        var seenNumbers = new HashSet<int>();
        for (var i = 0; i < orderedRequestParts.Count; i++) {
            var partNumber = orderedRequestParts[i].PartNumber;
            if (partNumber <= 0) throw new S3Exception(new Error(ErrorCode.InvalidPart));
            if (!seenNumbers.Add(partNumber)) throw new S3Exception(new Error(ErrorCode.InvalidPart));
            if (partNumber != i + 1) throw new S3Exception(new Error(ErrorCode.InvalidPartOrder));
        }

        if (upload.Parts.Count != orderedRequestParts.Count) {
            _logger.LogWarning(
                "Multipart completion mismatch for upload {UploadId}: provided {Provided} parts but stored {Stored}",
                upload.Id, orderedRequestParts.Count, upload.Parts.Count);
            throw new S3Exception(new Error(ErrorCode.InvalidPart));
        }

        var partsByNumber = upload.Parts.ToDictionary(p => p.PartNumber);
        foreach (var part in orderedRequestParts) {
            if (!partsByNumber.TryGetValue(part.PartNumber, out var storedPart)) {
                _logger.LogWarning("Missing part {PartNumber} for upload {UploadId}", part.PartNumber, upload.Id);
                throw new S3Exception(new Error(ErrorCode.InvalidPart));
            }

            if (!string.IsNullOrEmpty(part.ETag) && NormalizeEtag(part.ETag) != NormalizeEtag(storedPart.Etag)) {
                _logger.LogWarning("ETag mismatch for upload {UploadId} part {PartNumber}", upload.Id, part.PartNumber);
                throw new S3Exception(new Error(ErrorCode.InvalidPart));
            }
        }

        _logger.LogInformation(
            "Completing multipart upload {UploadId} for {Bucket}/{Key} with {PartCount} parts",
            upload.Id, ctx.Request.Bucket, ctx.Request.Key, orderedRequestParts.Count);

        var finalFilePath = Path.Combine(upload.UploadDirectory, $"{upload.Id}.final");
        long totalLength = 0;

        try {
            await using var finalStream =
                new FileStream(finalFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            foreach (var part in orderedRequestParts) {
                var storedPart = partsByNumber[part.PartNumber];
                await using var partStream = new FileStream(storedPart.TempFilePath, FileMode.Open, FileAccess.Read,
                    FileShare.Read);
                await partStream.CopyToAsync(finalStream, ctx.Http.Token);
                totalLength += storedPart.Size;
            }
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to assemble multipart upload {UploadId} for bucket {Bucket} key {Key}",
                upload.Id, ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        var latestObject = await _db.Objects.ReadObjectLatestMetadataAsync(bucket.Id, upload.Key, ctx.Http.Token);
        var replaceCurrent = latestObject is not null && !bucket.EnableVersioning;

        var (actorId, _) = ResolveActor(md, ctx, upload);
        var obj = new Object {
            BucketId = bucket.Id,
            AuthorId = actorId,
            OwnerId = actorId,
            Key = upload.Key,
            ContentType = string.IsNullOrWhiteSpace(upload.ContentType)
                ? "application/octet-stream"
                : upload.ContentType,
            ContentLength = totalLength,
            DeleteMarker = false,
            ExpirationUtc = null,
            Etag = string.Empty
        };

        obj.BlobFilename = obj.Id;
        obj.IsFolder = obj.ContentLength == 0 && obj.Key.EndsWith('/');
        obj.Version = latestObject is null ? 1 : latestObject.Version + 1;

        var uploadEncryptionRequest = BuildEncryptionRequest(upload);

        EncryptionOutcome? encryptionOutcome = null;

        try {
            encryptionOutcome = await PersistObjectAsync(ctx, md, obj, finalFilePath, uploadEncryptionRequest);
        }
        finally {
            DeleteFileIfExists(finalFilePath);
        }

        if (encryptionOutcome is not null) {
            ctx.Response.Headers[Constants.Headers.ServerSideEncryption] = encryptionOutcome.Algorithm;
        }
        else if (uploadEncryptionRequest is not null) {
            ctx.Response.Headers[Constants.Headers.ServerSideEncryption] = uploadEncryptionRequest.Algorithm;
        }

        _logger.LogInformation(
            "Multipart upload {UploadId} persisted as version {Version} for {Bucket}/{Key} ({Length} bytes)",
            upload.Id, obj.Version, ctx.Request.Bucket, ctx.Request.Key, obj.ContentLength);

        await ApplyAclHeadersAsync(ctx, md, obj);

        if (replaceCurrent && latestObject is not null) {
            await CleanupReplacedObjectAsync(ctx, bucket, latestObject);
        }

        foreach (var storedPart in upload.Parts) {
            DeleteFileIfExists(storedPart.TempFilePath);
        }

        _db.MultipartUploadParts.RemoveRange(upload.Parts);
        _db.MultipartUploads.Remove(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        SafeDeleteDirectory(upload.UploadDirectory);

        _logger.LogInformation("Multipart upload {UploadId} finalized for {Bucket}/{Key}", upload.Id,
            ctx.Request.Bucket, ctx.Request.Key);

        var etagHeader = string.IsNullOrEmpty(obj.Etag) ? null : $"\"{obj.Etag.ToLowerInvariant()}\"";
        if (!string.IsNullOrEmpty(etagHeader)) ctx.Response.Headers["ETag"] = etagHeader;

        return new CompleteMultipartUploadResult {
            Bucket = ctx.Request.Bucket,
            Key = ctx.Request.Key,
            Location = $"/{ctx.Request.Bucket}/{ctx.Request.Key}",
            ETag = etagHeader
        };
    }

    public async Task AbortMultipartUpload(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        var md = RequireMetadata(ctx, "multipart abort");
        EnsureAuthorized(md, ctx, "multipart abort");

        var upload = await GetMultipartUploadAsync(ctx, md);

        foreach (var part in upload.Parts) {
            DeleteFileIfExists(part.TempFilePath);
        }

        _db.MultipartUploadParts.RemoveRange(upload.Parts);
        _db.MultipartUploads.Remove(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        SafeDeleteDirectory(upload.UploadDirectory);

        _logger.LogInformation("Aborted multipart upload {UploadId} for {Bucket}/{Key}; removed {PartCount} parts",
            upload.Id, ctx.Request.Bucket, ctx.Request.Key, upload.Parts.Count);
    }
}