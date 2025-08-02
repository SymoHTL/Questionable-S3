namespace Application.QueryableExtensions;

public static class BucketTagExtensions {
    public static async Task<List<BucketTag>> ReadByBucketIdAsync(this IQueryable<BucketTag> bucketTags,
        string bucketId, CancellationToken ct = default) {
        return await bucketTags
            .Where(acl => acl.BucketId == bucketId)
            .ToListAsync(ct);
    }
}