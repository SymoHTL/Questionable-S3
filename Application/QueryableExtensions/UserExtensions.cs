namespace Application.QueryableExtensions;

public static class UserExtensions {
    public static async Task<User?>
        ReadUserById(this IQueryable<User> users, string id, CancellationToken ct = default) {
        return await users.FirstOrDefaultAsync(u => u.Id == id, ct);
    }
}