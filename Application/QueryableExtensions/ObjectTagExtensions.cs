namespace Application.QueryableExtensions;

public static class ObjectTagExtensions {
    public static async Task<List<ObjectTag>> ReadByObjectIdAsync(this IQueryable<ObjectTag> objectTags,
        string objectId, CancellationToken ct = default) {
        return await objectTags
            .Where(acl => acl.ObjectId == objectId)
            .ToListAsync(ct);
    }
    
    public static async Task<int> DeleteObjectVersionTagsAsync(this IQueryable<ObjectTag> objectTags,
        string bucketId, string objectId, long version, CancellationToken ct = default) {
        return await objectTags
            .Where(tag => tag.BucketId == bucketId && 
                          tag.ObjectId == objectId && 
                          tag.Object.Version == version)
            .ExecuteDeleteAsync(ct);
    }
}