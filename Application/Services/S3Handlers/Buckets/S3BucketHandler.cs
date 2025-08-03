using S3ServerLibrary.S3Objects;
using Bucket = Domain.Entities.Bucket;

namespace Application.Services.S3Handlers.Buckets;

public class S3BucketHandler : IS3BucketHandler {
    private readonly ILogger<S3BucketHandler> _logger;
    private readonly TimeProvider _tp;
    private readonly IDiscordService _discordService;
    private readonly IDbContext _db;

    public S3BucketHandler(ILogger<S3BucketHandler> logger, TimeProvider tp, IDiscordService discordService,
        IDbContext db) {
        _logger = logger;
        _tp = tp;
        _discordService = discordService;
        _db = db;
    }

    public async Task Write(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));
        RequestMetadata md = ((RequestMetadata?)ctx.Metadata)!;
        if (md is null) {
            _logger.LogWarning("Request metadata is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.User is null || md.Credential is null) {
            _logger.LogWarning("User or credential is null for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is not null) {
            _logger.LogWarning("Bucket already exists for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.BucketAlreadyExists));
        }

        if (IsInvalidBucketName(ctx.Request.Bucket)) {
            _logger.LogWarning("Invalid bucket name for request {@Request}", ctx.Request);
            throw new S3Exception(new Error(ErrorCode.InvalidRequest));
        }

        var bucket = new Bucket() {
            Name = ctx.Request.Bucket,
            OwnerGuid = md.User.Id,
            StorageType = EStorageDriverType.Discord,
            RegionString = "eu-west-1",
            CreatedUtc = _tp.GetUtcNow(),
        };
        bucket.ChannelId = await _discordService.CreateChannelAsync(bucket.Name);

        _db.Buckets.Add(bucket);
        await _db.SaveChangesAsync();

        List<Grant> grants = await Grants.GrantsFromHeaders(_db, md.User, ctx.Http.Request.Headers);
        if (grants is { Count: > 0 }) {
            foreach (Grant curr in grants) {
                if (curr.Grantee != null) {
                    BucketAcl? bucketAcl;
                    var permitRead = false;
                    var permitWrite = false;
                    var permitReadAcp = false;
                    var permitWriteAcp = false;
                    var fullControl = false;

                    if (!string.IsNullOrEmpty(curr.Grantee.ID)) {
                        var tempUser = await _db.Users.ReadUserByIdAsync(curr.Grantee.ID);
                        if (tempUser is null) {
                            _logger.LogWarning("Grantee user not found for ID {GranteeId} in request {@Request}",
                                curr.Grantee.ID, ctx.Request);
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

                        bucketAcl = new BucketAcl() {
                            UserId = tempUser.Id,
                            IssuedByUserId = md.User.Id,
                            BucketId = bucket.Id,
                            PermitRead = permitRead,
                            PermitWrite = permitWrite,
                            PermitReadAcp = permitReadAcp,
                            PermitWriteAcp = permitWriteAcp,
                            FullControl = fullControl,
                            UserGroup = null
                        };

                        _db.BucketAcls.Add(bucketAcl);
                    }
                    else if (!string.IsNullOrEmpty(curr.Grantee.URI)) {
                        if (curr.Permission == PermissionEnum.Read) permitRead = true;
                        else if (curr.Permission == PermissionEnum.Write) permitWrite = true;
                        else if (curr.Permission == PermissionEnum.ReadAcp) permitReadAcp = true;
                        else if (curr.Permission == PermissionEnum.WriteAcp) permitWriteAcp = true;
                        else if (curr.Permission == PermissionEnum.FullControl) fullControl = true;

                        
                        bucketAcl = new BucketAcl() {
                            UserId = null,
                            UserGroup = curr.Grantee.URI,
                            IssuedByUserId = md.User.Id,
                            BucketId = bucket.Id,
                            PermitRead = permitRead,
                            PermitWrite = permitWrite,
                            PermitReadAcp = permitReadAcp,
                            PermitWriteAcp = permitWriteAcp,
                            FullControl = fullControl,
                        };
                       
                        _db.BucketAcls.Add(bucketAcl);
                    }
                }
            }
            await _db.SaveChangesAsync();
        }
    }

    private readonly HashSet<string> _invalidNames = ["admin"];

    private bool IsInvalidBucketName(string name) {
        return _invalidNames.Contains(name.ToLower());
    }
}