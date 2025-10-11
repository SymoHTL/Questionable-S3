namespace Application.QueryableExtensions;

public static class MultipartUploadExtensions {
    public static Task<MultipartUpload?> ReadMultipartUploadAsync(
        this IQueryable<MultipartUpload> uploads,
        string bucketId,
        string key,
        string uploadId,
        CancellationToken ct = default) {
        return uploads
            .Include(u => u.Parts)
            .FirstOrDefaultAsync(u => u.BucketId == bucketId && u.Key == key && u.Id == uploadId, ct);
    }

    public static Task<List<MultipartUploadPart>> ReadUploadPartsAsync(
        this IQueryable<MultipartUploadPart> uploadParts,
        string uploadId,
        CancellationToken ct = default) {
        return uploadParts
            .Where(p => p.UploadId == uploadId)
            .OrderBy(p => p.PartNumber)
            .ToListAsync(ct);
    }
}
