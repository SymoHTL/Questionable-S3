namespace Application.QueryableExtensions;

public static class BucketAclExtensions {
    public static async Task<List<BucketAcl>> ReadByBucketIdAsync(this IQueryable<BucketAcl> bucketAcls,
        string bucketId, CancellationToken ct = default) {
        return await bucketAcls
            .Where(acl => acl.BucketId == bucketId)
            .ToListAsync(ct);
    }
}