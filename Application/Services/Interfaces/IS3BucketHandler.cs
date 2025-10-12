using S3ServerLibrary.S3Objects;

namespace Application.Services.Interfaces;

public interface IS3BucketHandler {
    Task Write(S3Context ctx);
    Task<ListAllMyBucketsResult> ListBuckets(S3Context ctx);
    Task<bool> Exists(S3Context ctx);
    Task Delete(S3Context ctx);
    Task<ListBucketResult> Read(S3Context ctx);
    Task<ListVersionsResult> ReadVersions(S3Context ctx);
    Task<AccessControlPolicy> ReadAcl(S3Context ctx);
    Task<Tagging> ReadTags(S3Context ctx);
    Task WriteTags(S3Context ctx, Tagging tagging);
    Task DeleteTags(S3Context ctx);
}