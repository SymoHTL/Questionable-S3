namespace Application.QueryableExtensions;

public static class BucketExtensions {
    public static async Task<Bucket?> ReadByNameAsync(this IQueryable<Bucket> buckets, string name,
        CancellationToken ct = default) {
        return await buckets.FirstOrDefaultAsync(b => b.Name == name, ct);
    }
}