namespace Infrastructure.S3Handlers.Auth;

public class S3AuthHandler {
    public RequestMetadata AuthenticateAndBuildMetadata(S3Context ctx) {
        ArgumentNullException.ThrowIfNull(ctx, nameof(ctx));

        RequestMetadata md = new RequestMetadata {
            Authentication = EAuthenticationResult.NotAuthenticated
        };

        #region Credential-and-User

        if (string.IsNullOrEmpty(ctx.Request.AccessKey)) {
            md.Authentication = EAuthenticationResult.NoMaterialSupplied;
        }
        else {
            Credential cred = _Config.GetCredentialByAccessKey(ctx.Request.AccessKey);
            if (cred == null) {
                md.Authentication = EAuthenticationResult.AccessKeyNotFound;
            }
            else {
                md.Credential = cred;

                User user = _Config.GetUserByAccessKey(ctx.Request.AccessKey);
                if (user == null) {
                    md.Authentication = EAuthenticationResult.UserNotFound;
                }
                else {
                    md.User = user;
                    md.Authentication = EAuthenticationResult.Authenticated;
                }
            }
        }

        #endregion

        #region Bucket

        if (!String.IsNullOrEmpty(ctx.Request.Bucket)) {
            md.Bucket = _Buckets.GetByName(ctx.Request.Bucket);

            if (md.Bucket != null) {
                md.BucketClient = _Buckets.GetClient(ctx.Request.Bucket);

                if (md.BucketClient != null) {
                    md.BucketAcls = md.BucketClient.GetBucketAcl();
                    md.BucketTags = md.BucketClient.GetBucketTags();
                }
            }
            else {
                if (md.Authentication == AuthenticationResult.Authenticated) {
                    md.Authorization = AuthorizationResult.PermitBucketOwnership;
                }
            }
        }

        #endregion

        #region Object

        if (md.BucketClient != null
            && ctx.Request.IsObjectRequest
            && !string.IsNullOrEmpty(ctx.Request.Key)) {
            if (string.IsNullOrEmpty(ctx.Request.VersionId)) {
                md.Obj = md.BucketClient.GetObjectLatestMetadata(ctx.Request.Key);
            }
            else {
                long versionId = 1;
                if (!string.IsNullOrEmpty(ctx.Request.VersionId)) {
                    long.TryParse(ctx.Request.VersionId, out versionId);
                }

                md.Obj = md.BucketClient.GetObjectVersionMetadata(ctx.Request.Key, versionId);
            }

            if (md.Obj != null) {
                md.ObjectAcls = md.BucketClient.GetObjectAcl(md.Obj.GUID);
                md.ObjectTags = md.BucketClient.GetObjectTags(md.Obj.GUID);
            }
            else {
                if (md.Authentication == AuthenticationResult.Authenticated) {
                    md.Authorization = AuthorizationResult.PermitObjectOwnership;
                }
            }
        }

        #endregion

        return md;
    }
}