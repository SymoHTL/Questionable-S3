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

    public S3ObjectHandler(ILogger<S3ObjectHandler> logger, IDbContext db, IBucketStore bucketStore,
        TimeProvider timeProvider) {
        _logger = logger;
        _db = db;
        _bucketStore = bucketStore;
        _timeProvider = timeProvider;
    }

    public async Task<S3Object> Read(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized request for bucket {Bucket} and key {Key}", ctx.Request.Bucket,
                ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            _logger.LogWarning("Bucket not found for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

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
        var latestVersion = (await _db.Objects.ReadObjectLatestMetadataAsync(md.Bucket.Id, md.Obj.Key))?.Version;
        if (md.Obj.Version < latestVersion) isLatest = false;

        var user = await _db.Users.ReadUserByIdAsync(md.Obj.OwnerId);
        var owner = user is not null ? new Owner(user.Id, user.Name) : null;

        var stream = await _bucketStore.GetObjectStreamAsync(md.Bucket, md.Obj, ctx.Http.Token);

        return new S3Object(md.Obj.Key, md.Obj.Version.ToString(), isLatest, md.Obj.LastUpdateUtc.UtcDateTime,
            md.Obj.Etag,
            md.Obj.ContentLength, owner, stream, md.Obj.ContentType);
    }


    public async Task Write(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;

        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized request for bucket {Bucket} and key {Key}", ctx.Request.Bucket,
                ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            _logger.LogWarning("Bucket not found for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        var obj = await _db.Objects.ReadObjectLatestMetadataAsync(
            md.Bucket.Id, ctx.Request.Key);
        if (obj is not null && !md.Bucket.EnableVersioning) {
            _logger.LogWarning("Versioning is disabled for bucket {Bucket}, prohibiting write to {Key}",
                ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InvalidBucketState));
        }

        #region Populate-Metadata

        if (obj is null) {
            // new object 
            obj = new Object();

            SetUser(md.User);

            obj.BucketId = md.Bucket.Id;
            obj.Version = 1;
            obj.BlobFilename = obj.Id;
            obj.ContentLength = ctx.Http.Request.ContentLength;
            obj.ContentType = ctx.Http.Request.ContentType;
            obj.DeleteMarker = false;
            obj.ExpirationUtc = null;
            obj.Key = ctx.Request.Key;

            if (obj.ContentLength == 0 && obj.Key.EndsWith('/')) obj.IsFolder = true;
        }
        else {
            // new version  
            SetUser(md.User);

            obj.Id = Guid.NewGuid().ToString();
            obj.BucketId = md.Bucket.Id;
            obj.Version += 1;
            obj.BlobFilename = obj.Id;
            obj.ContentLength = ctx.Http.Request.ContentLength;
            obj.ContentType = ctx.Http.Request.ContentType;
            obj.DeleteMarker = false;
            obj.ExpirationUtc = null;
            obj.Key = ctx.Request.Key;
        }

        void SetUser(User? user) {
            if (user is not null) {
                obj.AuthorId = user.Id;
                obj.OwnerId = user.Id;
            }
            else {
                obj.AuthorId = $"{ctx.Http.Request.Source.IpAddress}:{ctx.Http.Request.Source.Port}";
                obj.OwnerId = $"{ctx.Http.Request.Source.IpAddress}:{ctx.Http.Request.Source.Port}";
            }
        }

        #endregion

        #region Write-Data-to-Temp-and-to-Bucket

        var tempFilePath = $"{Constants.File.TempDir}/{Ulid.NewUlid()}";
        if (!Directory.Exists(Constants.File.TempDir)) {
            Directory.CreateDirectory(Constants.File.TempDir);
        }

        long totalLength = 0;
        var writeSuccess = false;

        try {
            await using (FileStream fs = new FileStream(tempFilePath, FileMode.Create)) {
                if (ctx.Request.Chunked) {
                    while (true) {
                        Chunk chunk = await ctx.Request.ReadChunk();
                        if (chunk == null) break;

                        if (chunk.Data is { Length: > 0 }) {
                            await fs.WriteAsync(chunk.Data.AsMemory(0, chunk.Data.Length));
                            totalLength += chunk.Data.Length;
                        }

                        if (chunk.IsFinal) break;
                    }
                }
                else {
                    if (ctx.Request.Data != null && ctx.Http.Request.ContentLength > 0) {
                        var bytesRemaining = ctx.Http.Request.ContentLength;
                        var buffer = new byte[65536];

                        while (bytesRemaining > 0) {
                            var bytesRead = await ctx.Request.Data.ReadAsync(buffer);
                            if (bytesRead <= 0) continue;

                            bytesRemaining -= bytesRead;
                            await fs.WriteAsync(buffer.AsMemory(0, bytesRead));
                        }

                        totalLength = obj.ContentLength;
                    }
                }
            }

            obj.ContentLength = totalLength;
            writeSuccess = await _bucketStore.AddObjectAsync(tempFilePath, md.Bucket, obj, ctx.Http.Token);
        }
        catch (Exception e) {
            _logger.LogError(e, "Error writing object {Bucket}/{Key} using tempfile {TempFilePath}",
                ctx.Request.Bucket, ctx.Request.Key, tempFilePath);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }
        finally {
            File.Delete(tempFilePath);
        }

        if (!writeSuccess) {
            _logger.LogWarning("Failed to write object {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        #endregion

        await ApplyAclHeadersAsync(ctx, md, obj);
    }

    public async Task Delete(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized request for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            _logger.LogWarning("Bucket not found for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        long versionId = 1;
        if (!string.IsNullOrEmpty(ctx.Request.VersionId)) {
            if (!long.TryParse(ctx.Request.VersionId, out versionId)) {
                throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
            }
        }

        if (md.Obj is null) {
            if (versionId == 1) {
                _logger.LogWarning("Object not found for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
                throw new S3Exception(new Error(ErrorCode.NoSuchKey));
            }

            _logger.LogWarning("Object version {VersionId} not found for bucket {Bucket} and key {Key}", versionId, ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.NoSuchVersion));
        }

        if (md.Obj.DeleteMarker) {
            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            throw new S3Exception(new Error(ErrorCode.NoSuchKey));
        }

        if (md.Bucket.EnableVersioning) {
            md.Obj.DeleteMarker = true;
            _db.Objects.Update(md.Obj);
            await _db.SaveChangesAsync();

            ctx.Response.Headers.Add(Constants.Headers.DeleteMarker, "true");
            return;
        }

        // Delete object version, ACL, and tags using _bucketStore and _db
        await _bucketStore.DeleteObjectVersionAsync(md.Bucket, md.Obj, versionId, ctx.Http.Token);
        await _db.ObjectAcls.DeleteObjectVersionAclAsync(md.Bucket.Id, md.Obj.Id, versionId);
        await _db.ObjectTags.DeleteObjectVersionTagsAsync(md.Bucket.Id, md.Obj.Id, versionId);
    }

    private async Task ApplyAclHeadersAsync(S3Context ctx, RequestMetadata md, Object obj) {
        if (md.User is null) return;

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
                    BucketId = md.Bucket.Id,
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
                    BucketId = md.Bucket.Id,
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

    private (string Id, string DisplayName) ResolveActor(RequestMetadata md, S3Context ctx, MultipartUpload? upload = null) {
        if (md.User is not null) return (md.User.Id, md.User.Name);

        if (upload is not null) return (upload.OwnerId, upload.OwnerDisplayName);

        var source = ctx.Http.Request.Source;
        var fallback = $"{source.IpAddress}:{source.Port}";
        return (fallback, fallback);
    }

    private async Task<MultipartUpload> GetMultipartUploadAsync(S3Context ctx, RequestMetadata md) {
        if (md.Bucket is null) {
            _logger.LogWarning("Bucket not found for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        if (string.IsNullOrEmpty(ctx.Request.UploadId)) {
            _logger.LogWarning("UploadId missing for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchUpload));
        }

        var upload = await _db.MultipartUploads.ReadMultipartUploadAsync(md.Bucket.Id, ctx.Request.Key, ctx.Request.UploadId, ctx.Http.Token);
        if (upload is null || upload.IsAborted) {
            _logger.LogWarning("Multipart upload {UploadId} not found for bucket {Bucket} and key {Key}", ctx.Request.UploadId, ctx.Request.Bucket, ctx.Request.Key);
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
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized multipart create for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            _logger.LogWarning("Bucket not found for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        var (ownerId, ownerName) = ResolveActor(md, ctx);
        var now = _timeProvider.GetUtcNow();

        var upload = new MultipartUpload {
            BucketId = md.Bucket.Id,
            Key = ctx.Request.Key,
            OwnerId = ownerId,
            OwnerDisplayName = ownerName,
            InitiatorId = ownerId,
            InitiatorDisplayName = ownerName,
            ContentType = string.IsNullOrWhiteSpace(ctx.Http.Request.ContentType)
                ? "application/octet-stream"
                : ctx.Http.Request.ContentType!,
            CreatedUtc = now,
            LastUpdatedUtc = now
        };

        upload.UploadDirectory = Path.Combine(Constants.File.TempDir, "multipart", md.Bucket.Id, upload.Id);
        EnsureUploadDirectory(upload);

        _db.MultipartUploads.Add(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        return new InitiateMultipartUploadResult(ctx.Request.Bucket, ctx.Request.Key, upload.Id);
    }

    public async Task UploadPart(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized multipart part upload for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        var upload = await GetMultipartUploadAsync(ctx, md);
        var partNumber = ctx.Request.PartNumber;
        if (partNumber <= 0) {
            _logger.LogWarning("Invalid part number {PartNumber} for upload {UploadId}", partNumber, ctx.Request.UploadId);
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
                    var bytesRead = await ctx.Request.Data.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken);
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
            _logger.LogError(ex, "Failed to compute checksum for multipart part {Bucket}/{Key} upload {UploadId} part {PartNumber}",
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
    }

    public async Task<ListPartsResult> ReadParts(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized multipart list for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

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
                _logger.LogWarning("Multipart part size exceeds supported range for upload {UploadId} part {PartNumber}", upload.Id, part.PartNumber);
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

    public async Task<CompleteMultipartUploadResult> CompleteMultipartUpload(S3Context ctx, CompleteMultipartUpload request) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        ArgumentNullException.ThrowIfNull(request, nameof(request));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized multipart complete for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (request.Parts is null || request.Parts.Count == 0) {
            _logger.LogWarning("Multipart completion request missing parts for upload {UploadId}", ctx.Request.UploadId);
            throw new S3Exception(new Error(ErrorCode.InvalidPart));
        }

        var upload = await GetMultipartUploadAsync(ctx, md);

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
            _logger.LogWarning("Multipart completion mismatch for upload {UploadId}: provided {Provided} parts but stored {Stored}",
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

        var finalFilePath = Path.Combine(upload.UploadDirectory, $"{upload.Id}.final");
        long totalLength = 0;

        try {
            await using var finalStream = new FileStream(finalFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            foreach (var part in orderedRequestParts) {
                var storedPart = partsByNumber[part.PartNumber];
                await using var partStream = new FileStream(storedPart.TempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                await partStream.CopyToAsync(finalStream, ctx.Http.Token);
                totalLength += storedPart.Size;
            }
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to assemble multipart upload {UploadId} for bucket {Bucket} key {Key}",
                upload.Id, ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        var latestObject = await _db.Objects.ReadObjectLatestMetadataAsync(md.Bucket.Id, upload.Key, ctx.Http.Token);
        if (latestObject is not null && !md.Bucket.EnableVersioning) {
            _logger.LogWarning("Versioning disabled for bucket {Bucket}; cannot complete multipart upload for existing object {Key}",
                ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InvalidBucketState));
        }

        var (actorId, _) = ResolveActor(md, ctx, upload);
        var obj = new Object {
            BucketId = md.Bucket.Id,
            AuthorId = actorId,
            OwnerId = actorId,
            Key = upload.Key,
            ContentType = string.IsNullOrWhiteSpace(upload.ContentType) ? "application/octet-stream" : upload.ContentType,
            ContentLength = totalLength,
            DeleteMarker = false,
            ExpirationUtc = null,
            Etag = string.Empty
        };

        obj.BlobFilename = obj.Id;
        obj.IsFolder = obj.ContentLength == 0 && obj.Key.EndsWith('/');
        obj.Version = latestObject is null ? 1 : latestObject.Version + 1;

        var writeSuccess = false;

        try {
            writeSuccess = await _bucketStore.AddObjectAsync(finalFilePath, md.Bucket, obj, ctx.Http.Token);
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to persist assembled multipart object {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }
        finally {
            DeleteFileIfExists(finalFilePath);
        }

        if (!writeSuccess) {
            _logger.LogWarning("Bucket store rejected assembled multipart object {Bucket}/{Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        await ApplyAclHeadersAsync(ctx, md, obj);

        foreach (var storedPart in upload.Parts) {
            DeleteFileIfExists(storedPart.TempFilePath);
        }

        _db.MultipartUploadParts.RemoveRange(upload.Parts);
        _db.MultipartUploads.Remove(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        SafeDeleteDirectory(upload.UploadDirectory);

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
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized multipart abort for bucket {Bucket} and key {Key}", ctx.Request.Bucket, ctx.Request.Key);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        var upload = await GetMultipartUploadAsync(ctx, md);

        foreach (var part in upload.Parts) {
            DeleteFileIfExists(part.TempFilePath);
        }

        _db.MultipartUploadParts.RemoveRange(upload.Parts);
        _db.MultipartUploads.Remove(upload);
        await _db.SaveChangesAsync(ctx.Http.Token);

        SafeDeleteDirectory(upload.UploadDirectory);
    }
}