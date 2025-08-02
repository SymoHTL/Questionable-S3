namespace Application.QueryableExtensions;

public static class CredentialExtensions {
    public static async Task<Credential?> ReadCredentialByAccessKeyAsync(this DbSet<Credential> db, string accessKey,
        CancellationToken ct = default) {
        return await db.FirstOrDefaultAsync(c => c.AccessKey == accessKey, ct);
    }
}