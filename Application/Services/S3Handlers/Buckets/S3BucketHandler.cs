using S3ServerLibrary.S3Objects;
using Bucket = Domain.Entities.Bucket;

namespace Application.Services.S3Handlers.Buckets;

public class S3BucketHandler : IS3BucketHandler {
    private readonly ILogger<S3BucketHandler> _logger;
    private readonly TimeProvider _tp;
    private readonly IDiscordService _discordService;
    private readonly IDbContext _db;
    private readonly IStorageMetrics _metrics;

    public S3BucketHandler(ILogger<S3BucketHandler> logger, TimeProvider tp, IDiscordService discordService,
        IDbContext db, IStorageMetrics metrics) {
        _logger = logger;
        _tp = tp;
        _discordService = discordService;
        _db = db;
        _metrics = metrics;
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
    _metrics.TrackBucketCreated();

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

    public async Task<ListAllMyBucketsResult> ListBuckets(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        if (ctx.Metadata is not RequestMetadata md) {
            _logger.LogWarning("Missing request metadata for ListBuckets");
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization != EAuthorizationResult.PermitService || md.User is null) {
            _logger.LogWarning("Unauthorized ListBuckets request from {Source}", ctx.Http.Request.Source);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        var cancellation = ctx.Http.Token;
        var userId = md.User.Id;

        var accessibleBuckets = await _db.Buckets
            .AsNoTracking()
            .Where(b =>
                b.OwnerGuid == userId ||
                b.EnablePublicRead ||
                _db.BucketAcls.Any(acl =>
                    acl.BucketId == b.Id &&
                    ((acl.PermitRead || acl.FullControl) &&
                     (
                         (!string.IsNullOrEmpty(acl.UserId) && acl.UserId == userId) ||
                         (!string.IsNullOrEmpty(acl.UserGroup) &&
                          (acl.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers) ||
                           acl.UserGroup.Contains(Constants.UserGroups.AllUsers)))
                     ))))
            .OrderBy(b => b.Name)
            .ToListAsync(cancellation);

        var s3Buckets = accessibleBuckets
            .Select(b => new S3ServerLibrary.S3Objects.Bucket(b.Name, b.CreatedUtc.UtcDateTime))
            .ToList();

        var owner = new Owner(md.User.Id, md.User.Name);
        var buckets = new S3ServerLibrary.S3Objects.Buckets(s3Buckets);

        return new ListAllMyBucketsResult(owner, buckets);
    }

    public async Task<bool> Exists(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        var cancellation = ctx.Http.Token;

        if (ctx.Metadata is RequestMetadata md && md.Bucket is not null)
            return true;

        if (string.IsNullOrWhiteSpace(ctx.Request.Bucket))
            return false;

        return await _db.Buckets
            .AsNoTracking()
            .AnyAsync(b => b.Name == ctx.Request.Bucket, cancellation);
    }

    public async Task Delete(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        if (ctx.Metadata is not RequestMetadata md) {
            _logger.LogWarning("Missing request metadata for bucket delete {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized delete for bucket {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            _logger.LogWarning("Bucket {Bucket} not found for delete", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        var cancellation = ctx.Http.Token;

        var bucket = await _db.Buckets
            .AsTracking()
            .FirstOrDefaultAsync(b => b.Id == md.Bucket.Id, cancellation);

        if (bucket is null) {
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        var hasObjects = await _db.Objects
            .AsNoTracking()
            .AnyAsync(o => o.BucketId == bucket.Id && !o.DeleteMarker, cancellation);

        if (hasObjects) {
            throw new S3Exception(new Error(ErrorCode.BucketNotEmpty));
        }

        if (bucket.ChannelId.HasValue) {
            try {
                await _discordService.DeleteChannelAsync(bucket.ChannelId.Value);
            }
            catch (Exception ex) {
                _logger.LogError(ex, "Failed to delete Discord channel {ChannelId} for bucket {Bucket}",
                    bucket.ChannelId, bucket.Name);
                throw new S3Exception(new Error(ErrorCode.InternalError));
            }
        }

        var bucketAcls = await _db.BucketAcls
            .AsTracking()
            .Where(a => a.BucketId == bucket.Id)
            .ToListAsync(cancellation);
        if (bucketAcls.Count > 0)
            _db.BucketAcls.RemoveRange(bucketAcls);

        var bucketTags = await _db.BucketTags
            .AsTracking()
            .Where(t => t.BucketId == bucket.Id)
            .ToListAsync(cancellation);
        if (bucketTags.Count > 0)
            _db.BucketTags.RemoveRange(bucketTags);

    _db.Buckets.Remove(bucket);
    await _db.SaveChangesAsync(cancellation);
    _metrics.TrackBucketDeleted();
    }

    public async Task<ListBucketResult> Read(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        if (ctx.Metadata is not RequestMetadata md) {
            _logger.LogWarning("Missing request metadata for bucket read {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized read for bucket {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null) {
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));
        }

        var cancellation = ctx.Http.Token;

        var bucket = await _db.Buckets
            .AsNoTracking()
            .FirstOrDefaultAsync(b => b.Id == md.Bucket.Id, cancellation) ?? md.Bucket;

        var prefix = ctx.Request.Prefix ?? string.Empty;
        var marker = ctx.Request.Marker ?? string.Empty;
        var requestedMaxKeys = ctx.Request.MaxKeys > 0 ? ctx.Request.MaxKeys : 1000;

        var query = _db.Objects
            .AsNoTracking()
            .Where(o => o.BucketId == bucket.Id && !o.DeleteMarker);

        if (!string.IsNullOrEmpty(prefix))
            query = query.Where(o => o.Key.StartsWith(prefix));

        if (!string.IsNullOrEmpty(marker))
            query = query.Where(o => string.CompareOrdinal(o.Key, marker) > 0);

        var objectEntities = await query
            .ToListAsync(cancellation);

        var latestObjects = objectEntities
            .GroupBy(o => o.Key, StringComparer.Ordinal)
            .Select(g => g.OrderByDescending(o => o.Version).First())
            .OrderBy(o => o.Key, StringComparer.Ordinal)
            .ToList();

        var ownerUser = await _db.Users
            .AsNoTracking()
            .FirstOrDefaultAsync(u => u.Id == bucket.OwnerGuid, cancellation);
        var owner = new Owner(ownerUser?.Id ?? bucket.OwnerGuid, ownerUser?.Name ?? bucket.OwnerGuid);

        var allContents = latestObjects
            .Select(o => new ObjectMetadata(
                o.Key,
                o.LastUpdateUtc.UtcDateTime,
                FormatEtag(o.Etag),
                o.ContentLength,
                owner,
                StorageClassEnum.STANDARD))
            .ToList();

        var limitedContents = requestedMaxKeys < int.MaxValue
            ? allContents.Take(requestedMaxKeys).ToList()
            : allContents;

        var isTruncated = allContents.Count > limitedContents.Count;
        var nextToken = isTruncated ? limitedContents.LastOrDefault()?.Key : null;

        var maxKeysValue = ctx.Request.MaxKeys > 0 ? ctx.Request.MaxKeys : limitedContents.Count;
        var commonPrefixes = new CommonPrefixes(new List<string>());

        return new ListBucketResult(
            bucket.Name,
            limitedContents,
            limitedContents.Count,
            maxKeysValue,
            prefix,
            marker,
            ctx.Request.Delimiter ?? string.Empty,
            isTruncated,
            nextToken,
            commonPrefixes,
            bucket.RegionString);
    }

    public async Task<ListVersionsResult> ReadVersions(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        if (ctx.Metadata is not RequestMetadata md) {
            _logger.LogWarning("Missing request metadata for ListVersions on {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized ListVersions request for bucket {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null)
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));

        var cancellation = ctx.Http.Token;

        var prefix = ctx.Request.Prefix ?? string.Empty;
        var keyMarker = ctx.Request.Marker ?? string.Empty;
        var versionMarker = ctx.Request.VersionId ?? string.Empty;
        var maxKeys = ctx.Request.MaxKeys > 0 ? ctx.Request.MaxKeys : 1000;

        var query = _db.Objects
            .AsNoTracking()
            .Where(o => o.BucketId == md.Bucket.Id);

        if (!string.IsNullOrEmpty(prefix))
            query = query.Where(o => o.Key.StartsWith(prefix));

        if (!string.IsNullOrEmpty(keyMarker)) {
            query = query.Where(o => string.CompareOrdinal(o.Key, keyMarker) > 0 ||
                                     (o.Key == keyMarker &&
                                      string.CompareOrdinal(o.Version.ToString(), versionMarker) > 0));
        }

        var allEntries = await query
            .OrderBy(o => o.Key)
            .ThenByDescending(o => o.Version)
            .ToListAsync(cancellation);

        var latestVersionLookup = allEntries
            .GroupBy(o => o.Key, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Max(x => x.Version), StringComparer.Ordinal);

        var ownerUser = await _db.Users
            .AsNoTracking()
            .FirstOrDefaultAsync(u => u.Id == md.Bucket.OwnerGuid, cancellation);
    var owner = new Owner(ownerUser?.Id ?? md.Bucket.OwnerGuid, ownerUser?.Name ?? md.Bucket.OwnerGuid);

        var flattened = new List<(Domain.Entities.Object Obj, bool IsDelete)>(allEntries.Count);
        flattened.AddRange(allEntries.Select(o => (o, o.DeleteMarker)));

        var limitedEntries = flattened.Take(maxKeys).ToList();
        var versions = new List<ObjectVersion>();
        var deleteMarkers = new List<DeleteMarker>();

        foreach (var entry in limitedEntries) {
            var obj = entry.Obj;
            var isLatest = latestVersionLookup.TryGetValue(obj.Key, out var latestVersion) &&
                           obj.Version == latestVersion;

            if (entry.IsDelete) {
                deleteMarkers.Add(new DeleteMarker(
                    obj.Key,
                    obj.Version.ToString(),
                    isLatest,
                    obj.LastUpdateUtc.UtcDateTime,
                    owner));
            }
            else {
                deleteMarkers.RemoveAll(dm => dm.Key == obj.Key && dm.VersionId == obj.Version.ToString());

                versions.Add(new ObjectVersion(
                    obj.Key,
                    obj.Version.ToString(),
                    isLatest,
                    obj.LastUpdateUtc.UtcDateTime,
                    FormatEtag(obj.Etag),
                    obj.ContentLength,
                    owner,
                    StorageClassEnum.STANDARD));
            }
        }

        var isTruncated = flattened.Count > limitedEntries.Count;

        return new ListVersionsResult(
            md.Bucket.Name,
            versions,
            deleteMarkers,
            maxKeys,
            prefix,
            keyMarker,
            versionMarker,
            isTruncated,
            md.Bucket.RegionString);
    }

    public async Task<AccessControlPolicy> ReadAcl(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx);

        if (ctx.Metadata is not RequestMetadata md) {
            _logger.LogWarning("Missing request metadata for ReadAcl on bucket {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.InternalError));
        }

        if (md.Authorization == EAuthorizationResult.NotAuthorized) {
            _logger.LogWarning("Unauthorized ReadAcl request for bucket {Bucket}", ctx.Request.Bucket);
            throw new S3Exception(new Error(ErrorCode.AccessDenied));
        }

        if (md.Bucket is null)
            throw new S3Exception(new Error(ErrorCode.NoSuchBucket));

        var cancellation = ctx.Http.Token;

        var ownerUser = await _db.Users
            .AsNoTracking()
            .FirstOrDefaultAsync(u => u.Id == md.Bucket.OwnerGuid, cancellation);
        var owner = new Owner(ownerUser?.Id ?? md.Bucket.OwnerGuid, ownerUser?.Name ?? md.Bucket.OwnerGuid);

        var bucketAcls = await _db.BucketAcls
            .AsNoTracking()
            .Where(a => a.BucketId == md.Bucket.Id)
            .ToListAsync(cancellation);

        var userIds = bucketAcls
            .Where(a => !string.IsNullOrWhiteSpace(a.UserId))
            .Select(a => a.UserId!)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var users = userIds.Length == 0
            ? new Dictionary<string, User>(StringComparer.Ordinal)
            : await _db.Users
                .AsNoTracking()
                .Where(u => userIds.Contains(u.Id))
                .ToDictionaryAsync(u => u.Id, StringComparer.Ordinal, cancellation);

        var grants = new List<Grant>();

        // Owner always has full control.
        grants.Add(new Grant(
            new Grantee(owner.ID, owner.DisplayName, null, "CanonicalUser", null),
            PermissionEnum.FullControl));

        foreach (var acl in bucketAcls) {
            Grantee grantee;
            if (!string.IsNullOrWhiteSpace(acl.UserId) && users.TryGetValue(acl.UserId!, out var user)) {
                grantee = new Grantee(user.Id, user.Name, null, "CanonicalUser", null);
            }
            else if (!string.IsNullOrWhiteSpace(acl.UserGroup)) {
                var display = ResolveGroupDisplayName(acl.UserGroup!);
                grantee = new Grantee(null, display, acl.UserGroup, "Group", null);
            }
            else {
                continue;
            }

            AddGrantIf(grants, grantee, PermissionEnum.Read, acl.PermitRead);
            AddGrantIf(grants, grantee, PermissionEnum.Write, acl.PermitWrite);
            AddGrantIf(grants, grantee, PermissionEnum.ReadAcp, acl.PermitReadAcp);
            AddGrantIf(grants, grantee, PermissionEnum.WriteAcp, acl.PermitWriteAcp);
            AddGrantIf(grants, grantee, PermissionEnum.FullControl, acl.FullControl);
        }

        var aclList = new AccessControlList(grants);
        return new AccessControlPolicy(owner, aclList);
    }

    private readonly HashSet<string> _invalidNames = ["admin"];

    private bool IsInvalidBucketName(string name) {
        return _invalidNames.Contains(name.ToLower());
    }

    private static string FormatEtag(string? etag) {
        if (string.IsNullOrWhiteSpace(etag)) return string.Empty;
        etag = etag.Trim('"');
        return $"\"{etag.ToLowerInvariant()}\"";
    }

    private static void AddGrantIf(List<Grant> grants, Grantee grantee, PermissionEnum permission, bool condition) {
        if (!condition) return;
        grants.Add(new Grant(
            new Grantee(grantee.ID, grantee.DisplayName, grantee.URI, grantee.GranteeType, grantee.EmailAddress),
            permission));
    }

    private static string ResolveGroupDisplayName(string uri) {
        if (uri.Contains(Constants.UserGroups.AllUsers, StringComparison.OrdinalIgnoreCase))
            return Constants.UserGroups.AllUsers;
        if (uri.Contains(Constants.UserGroups.AuthenticatedUsers, StringComparison.OrdinalIgnoreCase))
            return Constants.UserGroups.AuthenticatedUsers;
        return uri;
    }
}