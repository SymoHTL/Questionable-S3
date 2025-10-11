namespace Application.QueryableExtensions;

public static class ObjectAclExtensions {
    public static async Task<List<ObjectAcl>> ReadByObjectIdAsync(this IQueryable<ObjectAcl> objectAcls,
        string objectId, CancellationToken ct = default) {
        return await objectAcls
            .Where(acl => acl.ObjectId == objectId)
            .ToListAsync(ct);
    }
    
    public static async Task<int> DeleteObjectVersionAclAsync(this IQueryable<ObjectAcl> objectAcls,
        string bucketId, string objectId, long version, CancellationToken ct = default) {
       return await objectAcls
            .Where(acl => acl.BucketId == bucketId && 
                          acl.ObjectId == objectId && 
                          acl.Object.Version == version)
            .ExecuteDeleteAsync(ct);
    }
}