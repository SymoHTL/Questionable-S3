using Microsoft.AspNetCore.DataProtection;

namespace Infrastructure.Configuration;

public static class DatabaseRegistrar {
    public static void AddDatabase(this IServiceCollection services, IConfiguration config) {
        services.AddDbContextFactory<QuestionableDbContext>(options => {
            options.UseMySql(
                config.GetConnectionString("DefaultConnection"),
                new MySqlServerVersion(new Version(8, 0, 29)),
                optionsBuilder => {
                    optionsBuilder.UseQuerySplittingBehavior(QuerySplittingBehavior.SplitQuery);
                    optionsBuilder.EnableIndexOptimizedBooleanColumns();
                    optionsBuilder.EnableStringComparisonTranslations();
                }
            );
            options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);
            options.EnableDetailedErrors();
        });

        services.AddDataProtection()
            .PersistKeysToDbContext<QuestionableDbContext>();

        services.AddTransient<IDbContext>(p =>
            p.GetRequiredService<IDbContextFactory<QuestionableDbContext>>().CreateDbContext());
    }

    public static async Task UseDatabaseAsync(this IServiceProvider serviceProvider, IConfiguration config) {
        await using var db = await serviceProvider.GetRequiredService<IDbContextFactory<QuestionableDbContext>>()
            .CreateDbContextAsync();
        await db.Database.EnsureCreatedAsync();

        if (!await db.Users.AnyAsync()) {
            var user = config.GetSection("SeedData:User")
                .Get<User>() ?? throw new InvalidOperationException("Seed data for User is not configured.");
            user.Id = Ulid.NewUlid().ToString();
            user.CreatedUtc = TimeProvider.System.GetUtcNow();

            db.Users.Add(user);

            var credential = config.GetSection("SeedData:Credential")
                                 .Get<Credential>() ??
                             throw new InvalidOperationException("Seed data for Credential is not configured.");
            credential.Id = Ulid.NewUlid().ToString();
            credential.UserId = user.Id;
            credential.CreatedUtc = TimeProvider.System.GetUtcNow();
            db.Credentials.Add(credential);

            await db.SaveChangesAsync();
        }
    }
}