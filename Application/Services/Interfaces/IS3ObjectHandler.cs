using S3ServerLibrary.S3Objects;

namespace Application.Services.Interfaces;

public interface IS3ObjectHandler {
    Task<S3Object> Read(S3Context ctx);
    Task Write(S3Context ctx);
    Task Delete(S3Context ctx);
    Task<Tagging> ReadTags(S3Context ctx);
    Task WriteTags(S3Context ctx, Tagging tagging);
    Task DeleteTags(S3Context ctx);
    Task<InitiateMultipartUploadResult> CreateMultipartUpload(S3Context ctx);
    Task UploadPart(S3Context ctx);
    Task<ListPartsResult> ReadParts(S3Context ctx);
    Task<CompleteMultipartUploadResult> CompleteMultipartUpload(S3Context ctx, CompleteMultipartUpload upload);
    Task AbortMultipartUpload(S3Context ctx);
}