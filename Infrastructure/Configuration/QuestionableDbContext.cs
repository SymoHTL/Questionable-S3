namespace Infrastructure.Configuration;

public class QuestionableDbContext : DbContext, IDbContext {
    public QuestionableDbContext(DbContextOptions<QuestionableDbContext> options) : base(options) {
        
    }
}