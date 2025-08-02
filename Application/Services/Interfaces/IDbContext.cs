namespace Application.Services.Interfaces;

public interface IDbContext {
    DbSet<Credential> Credentials { get; }
    DbSet<User> Users { get; }
    DbSet<Bucket> Buckets { get; }
    DbSet<BucketAcl> BucketAcls { get; }
    DbSet<BucketTag> BucketTags { get; }
    DbSet<DcObject> DcObjects { get; }
    DbSet<ObjectAcl> ObjectAcls { get; }
    DbSet<ObjectTag> ObjectTags { get; }
    public Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
}