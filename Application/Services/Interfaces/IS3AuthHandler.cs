namespace Application.Services.Interfaces;

public interface IS3AuthHandler {
    Task<RequestMetadata> AuthenticateAndBuildMetadataAsync(S3Context ctx);
    RequestMetadata AuthorizeServiceRequest(S3Context ctx, RequestMetadata md);
    RequestMetadata AuthorizeBucketRequest(S3Context ctx, RequestMetadata md);
    RequestMetadata AuthorizeObjectRequest(S3Context ctx, RequestMetadata md);
}