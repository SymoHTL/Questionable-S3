namespace Application.Services.Interfaces;

public interface IDbContext {
    DbSet<Credential> Credentials { get; }
    DbSet<User> Users { get; }
    DbSet<Bucket> Buckets { get; }
    DbSet<BucketAcl> BucketAcls { get; }
    DbSet<BucketTag> BucketTags { get; }
    DbSet<Object> Objects { get; }
    DbSet<ObjectAcl> ObjectAcls { get; }
    DbSet<ObjectTag> ObjectTags { get; }
    DbSet<ObjectChunk> ObjectChunks { get; set; }
    DbSet<MultipartUpload> MultipartUploads { get; }
    DbSet<MultipartUploadPart> MultipartUploadParts { get; }
    public Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
}