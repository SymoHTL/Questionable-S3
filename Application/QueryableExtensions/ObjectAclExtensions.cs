namespace Application.QueryableExtensions;

public static class ObjectAclExtensions {
    public static async Task<List<ObjectAcl>> ReadByObjectIdAsync(this IQueryable<ObjectAcl> objectAcls,
        string objectId, CancellationToken ct = default) {
        return await objectAcls
            .Where(acl => acl.ObjectId == objectId)
            .ToListAsync(ct);
    }
}