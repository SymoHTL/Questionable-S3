namespace Application.QueryableExtensions;

public static class DbObjectExtensions {
    public static async Task<Object?> ReadObjectLatestMetadataAsync(
        this IQueryable<Object> objects, string bucketId, string key, CancellationToken ct = default) {
        return await objects
            .Where(o => o.BucketId == bucketId && o.Key == key)
            .OrderByDescending(o => o.Version)
            .FirstOrDefaultAsync(ct);
    }

    public static async Task<Object?> ReadObjectByKeyAndVersionAsync(this IQueryable<Object> objects, string bucketId,
        string key,
        long version, CancellationToken ct = default) {
        return await objects
            .FirstOrDefaultAsync(o => o.BucketId == bucketId &&
                                      o.Key == key &&
                                      o.Version == version, ct);
    }

    public static async Task<Object?> ReadObjectMetadataByIdAsync(
        this IQueryable<Object> objects, string bucketId, string id, CancellationToken ct = default) {
        return await objects
            .FirstOrDefaultAsync(o => o.BucketId == bucketId && o.Id == id, ct);
    }
}