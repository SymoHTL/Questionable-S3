namespace Infrastructure.Configuration;

public class QuestionableDbContext : DbContext, IDbContext, IDataProtectionKeyContext {
    public QuestionableDbContext(DbContextOptions<QuestionableDbContext> options) : base(options) { }

    public DbSet<Credential> Credentials { get; set; }
    public DbSet<User> Users { get; set; }
    public DbSet<Bucket> Buckets { get; set; }
    public DbSet<BucketAcl> BucketAcls { get; set; }
    public DbSet<BucketTag> BucketTags { get; set; }
    public DbSet<Object> Objects { get; set; }
    public DbSet<ObjectAcl> ObjectAcls { get; set; }
    public DbSet<ObjectTag> ObjectTags { get; set; }
    public DbSet<ObjectChunk> ObjectChunks { get; set; }
    public DbSet<MultipartUpload> MultipartUploads { get; set; }
    public DbSet<MultipartUploadPart> MultipartUploadParts { get; set; }
    
    public DbSet<DataProtectionKey> DataProtectionKeys { get; set; }
    
    
    protected override void OnModelCreating(ModelBuilder builder) {
        base.OnModelCreating(builder);

        builder.ApplyConfigurationsFromAssembly(typeof(AssemblyPointer).Assembly);
    }
    
}