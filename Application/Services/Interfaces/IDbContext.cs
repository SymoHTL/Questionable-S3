namespace Application.Services.Interfaces;

public interface IDbContext {
    public Task<int> SaveChangesAsync(CancellationToken cancellationToken = default(CancellationToken));
}