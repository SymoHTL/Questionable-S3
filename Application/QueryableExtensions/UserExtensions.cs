namespace Application.QueryableExtensions;

public static class UserExtensions {
    public static async Task<User?> ReadUserByEmailAsync(this IQueryable<User> users, string email, CancellationToken ct = default) {
        return await users.FirstOrDefaultAsync(u => u.Email == email, ct);
    }
    
    public static async Task<User?> ReadUserByIdAsync(this IQueryable<User> users, string id, CancellationToken ct = default) {
        return await users.FirstOrDefaultAsync(u => u.Id == id, ct);
    }
}