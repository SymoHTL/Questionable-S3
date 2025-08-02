namespace Infrastructure.Configuration;

public static class DatabaseRegistrar {
    public static void AddDatabase(this IServiceCollection services, IConfiguration config) {
        services.AddSqlite<QuestionableDbContext>(config.GetConnectionString("DefaultConnection"),
            sqliteOptions => {
                sqliteOptions.UseQuerySplittingBehavior(QuerySplittingBehavior.SplitQuery);
            }, options => {
                options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);
                options.EnableDetailedErrors();
            });
    }

    public static async Task UseDatabaseAsync(this IServiceProvider serviceProvider) {
        using var scope = serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<QuestionableDbContext>();
        await dbContext.Database.EnsureCreatedAsync();
        await dbContext.Database.MigrateAsync();
    }
}