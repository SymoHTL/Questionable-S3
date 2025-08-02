namespace Application.Services.S3Handlers.Auth;

public class S3AuthHandler : IS3AuthHandler {
    private readonly IDbContext _dbContext;

    public S3AuthHandler(IDbContext dbContext) {
        _dbContext = dbContext;
    }


    public async Task<RequestMetadata> AuthenticateAndBuildMetadataAsync(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));

        var md = new RequestMetadata {
            Authentication = EAuthenticationResult.NotAuthenticated
        };


        if (string.IsNullOrEmpty(ctx.Request.AccessKey)) {
            md.Authentication = EAuthenticationResult.NoMaterialSupplied;
        }
        else {
            var cred = await _dbContext.Credentials.ReadCredentialByAccessKeyAsync(ctx.Request.AccessKey);
            if (cred is null) {
                md.Authentication = EAuthenticationResult.AccessKeyNotFound;
            }
            else {
                md.Credential = cred;

                var user = await _dbContext.Users.ReadUserById(cred.UserId);
                if (user is null) {
                    md.Authentication = EAuthenticationResult.UserNotFound;
                }
                else {
                    md.User = user;
                    md.Authentication = EAuthenticationResult.Authenticated;
                }
            }
        }


        if (!string.IsNullOrEmpty(ctx.Request.Bucket)) {
            md.Bucket = await _dbContext.Buckets.ReadByNameAsync(ctx.Request.Bucket);

            if (md.Bucket is not null) {
                md.BucketAcls = await _dbContext.BucketAcls.ReadByBucketIdAsync(md.Bucket.Id);
                md.BucketTags = await _dbContext.BucketTags.ReadByBucketIdAsync(md.Bucket.Id);
            }
            else if (md.Authentication is EAuthenticationResult.Authenticated) {
                md.Authorization = EAuthorizationResult.PermitBucketOwnership;
            }
        }


        if (md.Bucket is not null && ctx.Request.IsObjectRequest && !string.IsNullOrEmpty(ctx.Request.Key)) {
            if (string.IsNullOrEmpty(ctx.Request.VersionId)) {
                md.Obj = await _dbContext.DcObjects.ReadObjectLatestMetadataAsync(md.Bucket.Id, ctx.Request.Key);
            }
            else {
                long versionId = 1;
                if (!string.IsNullOrEmpty(ctx.Request.VersionId)) long.TryParse(ctx.Request.VersionId, out versionId);

                md.Obj = await _dbContext.DcObjects.ReadObjectByKeyAndVersionAsync(ctx.Request.Key, versionId);
            }

            if (md.Obj != null) {
                md.ObjectAcls = await _dbContext.ObjectAcls.ReadByObjectIdAsync(md.Obj.Id);
                md.ObjectTags = await _dbContext.ObjectTags.ReadByObjectIdAsync(md.Obj.Id);
            }
            else {
                if (md.Authentication is EAuthenticationResult.Authenticated)
                    md.Authorization = EAuthorizationResult.PermitObjectOwnership;
            }
        }

        return md;
    }

    public RequestMetadata AuthorizeServiceRequest(S3Context ctx, RequestMetadata md) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(md);

        md.Authorization = EAuthorizationResult.NotAuthorized;

        if (md is { User: not null, Authentication: EAuthenticationResult.Authenticated })
            md.Authorization = EAuthorizationResult.PermitService;

        return md;
    }

    public RequestMetadata AuthorizeBucketRequest(S3Context ctx, RequestMetadata md) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(md);

        var allowed = false;

        #region Check-for-Bucket-Write

        if (ctx.Request.RequestType is S3RequestType.BucketWrite &&
            md.Authentication is EAuthenticationResult.Authenticated) {
            md.Authorization = EAuthorizationResult.PermitBucketOwnership;
            return md;
        }

        #endregion


        #region Check-for-Bucket-Global-Config

        if (md.Bucket != null)
            switch (ctx.Request.RequestType) {
                case S3RequestType.BucketExists:
                case S3RequestType.BucketRead:
                case S3RequestType.BucketReadVersioning:
                case S3RequestType.BucketReadVersions:
                    if (md.Bucket.EnablePublicRead) {
                        md.Authorization = EAuthorizationResult.PermitBucketGlobalConfig;
                        return md;
                    }

                    break;

                case S3RequestType.BucketDeleteTags:
                case S3RequestType.BucketWriteTags:
                case S3RequestType.BucketWriteVersioning:
                    if (md.Bucket.EnablePublicWrite) {
                        md.Authorization = EAuthorizationResult.PermitBucketGlobalConfig;
                        return md;
                    }

                    break;
            }

        #endregion

        #region Check-for-Bucket-AllUsers-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.BucketExists:
                case S3RequestType.BucketRead:
                case S3RequestType.BucketReadVersioning:
                case S3RequestType.BucketReadVersions:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.BucketReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.BucketDelete:
                case S3RequestType.BucketDeleteTags:
                case S3RequestType.BucketWrite:
                case S3RequestType.BucketWriteTags:
                case S3RequestType.BucketWriteVersioning:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.BucketWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketAllUsersAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Auth-Material

        if (md.User is null || md.Credential is null) {
            md.Authorization = EAuthorizationResult.NotAuthorized;
            return md;
        }

        #endregion

        #region Check-for-Bucket-Owner

        if (md.Bucket != null && md.Bucket.OwnerGuid.Equals(md.User.Id)) {
            md.Authorization = EAuthorizationResult.PermitBucketOwnership;
            return md;
        }

        #endregion

        #region Check-for-Bucket-AuthenticatedUsers-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.BucketExists:
                case S3RequestType.BucketRead:
                case S3RequestType.BucketReadVersioning:
                case S3RequestType.BucketReadVersions:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.BucketReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.BucketDelete:
                case S3RequestType.BucketDeleteTags:
                case S3RequestType.BucketWrite:
                case S3RequestType.BucketWriteTags:
                case S3RequestType.BucketWriteVersioning:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.BucketWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketAuthUserAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Bucket-User-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.BucketExists:
                case S3RequestType.BucketRead:
                case S3RequestType.BucketReadVersioning:
                case S3RequestType.BucketReadVersions:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.BucketReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.BucketDelete:
                case S3RequestType.BucketDeleteTags:
                case S3RequestType.BucketWrite:
                case S3RequestType.BucketWriteTags:
                case S3RequestType.BucketWriteVersioning:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.BucketWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketUserAcl;
                return md;
            }
        }

        #endregion

        return md;
    }

    public RequestMetadata AuthorizeObjectRequest(S3Context ctx, RequestMetadata md) {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(md);

        var allowed = false;

        #region Get-Version-ID

        long versionId = 1;
        if (!string.IsNullOrEmpty(ctx.Request.VersionId))
            if (!long.TryParse(ctx.Request.VersionId, out versionId)) { }

        #endregion

        #region Check-for-Bucket-Global-Config

        if (md.Bucket is not null)
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    if (md.Bucket.EnablePublicRead) allowed = true;
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    if (md.Bucket.EnablePublicWrite) allowed = true;
                    break;
            }

        if (allowed) {
            md.Authorization = EAuthorizationResult.PermitBucketGlobalConfig;
            return md;
        }

        #endregion

        #region Check-for-Bucket-AllUsers-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketAllUsersAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Object-AllUsers-ACL

        if (md.ObjectAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                // case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AllUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitObjectAllUsersAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Auth-Material

        if (md.User is null || md.Credential is null) {
            md.Authorization = EAuthorizationResult.NotAuthorized;
            return md;
        }

        #endregion

        #region Check-for-Bucket-Owner

        if (md.Bucket is not null)
            if (md.Bucket.OwnerGuid.Equals(md.User.Id)) {
                md.Authorization = EAuthorizationResult.PermitBucketOwnership;
                return md;
            }

        #endregion

        #region Check-for-Object-Owner

        if (md.Obj is not null)
            if (md.Obj.OwnerId.Equals(md.User.Id)) {
                md.Authorization = EAuthorizationResult.PermitObjectOwnership;
                return md;
            }

        #endregion

        #region Check-for-Bucket-AuthenticatedUsers-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketAuthUserAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Object-AuthenticatedUsers-ACL

        if (md.ObjectAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                // case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserGroup)
                                                        && b.UserGroup.Contains(Constants.UserGroups.AuthenticatedUsers)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitObjectAuthUserAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Bucket-User-ACL

        if (md.BucketAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.BucketAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitBucketUserAcl;
                return md;
            }
        }

        #endregion

        #region Check-for-Object-User-ACL

        if (md.ObjectAcls is { Count: > 0 }) {
            switch (ctx.Request.RequestType) {
                case S3RequestType.ObjectExists:
                case S3RequestType.ObjectRead:
                case S3RequestType.ObjectReadLegalHold:
                case S3RequestType.ObjectReadRange:
                case S3RequestType.ObjectReadRetention:
                case S3RequestType.ObjectReadTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitRead || b.FullControl));
                    break;

                case S3RequestType.ObjectReadAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitReadAcp || b.FullControl));
                    break;

                case S3RequestType.ObjectDelete:
                case S3RequestType.ObjectDeleteMultiple:
                case S3RequestType.ObjectDeleteTags:
                // case S3RequestType.ObjectWrite:
                case S3RequestType.ObjectWriteLegalHold:
                case S3RequestType.ObjectWriteRetention:
                case S3RequestType.ObjectWriteTags:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWrite || b.FullControl));
                    break;

                case S3RequestType.ObjectWriteAcl:
                    allowed = md.ObjectAcls.Exists(b => !string.IsNullOrEmpty(b.UserId)
                                                        && b.UserId.Equals(md.User.Id)
                                                        && (b.PermitWriteAcp || b.FullControl));
                    break;
            }

            if (allowed) {
                md.Authorization = EAuthorizationResult.PermitObjectUserAcl;
                return md;
            }
        }

        #endregion

        return md;
    }
}