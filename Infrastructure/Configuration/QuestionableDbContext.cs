namespace Infrastructure.Configuration;

public class QuestionableDbContext : DbContext, IDbContext {
    public QuestionableDbContext(DbContextOptions<QuestionableDbContext> options) : base(options) { }

    public DbSet<Credential> Credentials { get; set; }
    public DbSet<User> Users { get; set; }
    public DbSet<Bucket> Buckets { get; set; }
    public DbSet<BucketAcl> BucketAcls { get; set; }
    public DbSet<BucketTag> BucketTags { get; set; }
    public DbSet<DcObject> DcObjects { get; set; }
    public DbSet<ObjectAcl> ObjectAcls { get; set; }
    public DbSet<ObjectTag> ObjectTags { get; set; }
}