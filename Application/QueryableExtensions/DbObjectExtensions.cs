namespace Application.QueryableExtensions;

public static class DbObjectExtensions {
    public static async Task<DcObject?> ReadObjectLatestMetadataAsync(
        this IQueryable<DcObject> objects, string bucketId, string key, CancellationToken ct = default) {
        return await objects
            .Where(o => o.BucketId == bucketId && o.Key == key)
            .OrderByDescending(o => o.Version)
            .FirstOrDefaultAsync(ct);
    }

    public static async Task<DcObject?> ReadObjectByKeyAndVersionAsync(
        this IQueryable<DcObject> objects, string key, long version, CancellationToken ct = default) {
        return await objects
            .Where(o => o.Key == key && o.Version == version)
            .FirstOrDefaultAsync(ct);
    }
}