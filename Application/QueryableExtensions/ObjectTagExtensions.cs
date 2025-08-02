namespace Application.QueryableExtensions;

public static class ObjectTagExtensions {
    public static async Task<List<ObjectTag>> ReadByObjectIdAsync(this IQueryable<ObjectTag> objectTags,
        string objectId, CancellationToken ct = default) {
        return await objectTags
            .Where(acl => acl.ObjectId == objectId)
            .ToListAsync(ct);
    }
}