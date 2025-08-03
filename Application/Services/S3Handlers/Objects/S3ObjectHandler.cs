using System.Collections.Specialized;
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
        
        return new S3Object(md.Obj.Key, md.Obj.Version.ToString(), isLatest, md.Obj.LastUpdateUtc.UtcDateTime, md.Obj.Etag,
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

        #region Permissions-in-Headers

        if (md.User is not null) {
            List<Grant> grants = await Grants.GrantsFromHeaders(_db, md.User, ctx.Http.Request.Headers);
            if (grants is { Count: > 0 }) {
                foreach (Grant curr in grants) {
                    if (curr.Grantee is not null) {
                        ObjectAcl objectAcl;
                        var permitRead = false;
                        var permitWrite = false;
                        var permitReadAcp = false;
                        var permitWriteAcp = false;
                        var fullControl = false;

                        if (!String.IsNullOrEmpty(curr.Grantee.ID)) {
                            var tempUser = await _db.Users.ReadUserByIdAsync(curr.Grantee.ID);
                            if (tempUser is null) {
                                _logger.LogWarning(
                                    "Unable to retrieve user {UserId} to add ACL to object {Bucket}/{Key} version {Version}",
                                    curr.Grantee.ID, ctx.Request.Bucket, ctx.Request.Key, obj.Version);
                                continue;
                            }

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

                            objectAcl = new ObjectAcl() {
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


                            objectAcl = new ObjectAcl() {
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
                                UserId = null!,
                            };

                            _db.ObjectAcls.Add(objectAcl);
                        }
                    }
                }

                await _db.SaveChangesAsync();
            }
        }

        #endregion
    }

    public async Task Delete(S3Context ctx) { }
}