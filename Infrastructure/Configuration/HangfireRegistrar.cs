using Hangfire;
using Hangfire.Redis.StackExchange;

namespace Infrastructure.Configuration;

public static class HangfireRegistrar {
    public static void AddHangfire(this IServiceCollection services, IConfiguration configuration) {
        var prefix = configuration.GetValue<string?>("RedisPrefix");
        if (string.IsNullOrEmpty(prefix))
            throw new InvalidOperationException("RedisPrefix configuration is required for Hangfire.");
        var connectionString = configuration.GetConnectionString("Redis");
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("Redis connection string is required for Hangfire.");

        services.AddHangfire(c => {
            c.UseSimpleAssemblyNameTypeSerializer();
            c.UseRecommendedSerializerSettings();
            c.UseRedisStorage(connectionString,
                new RedisStorageOptions { Prefix = prefix });
        });

        services.AddHangfireServer();
    }
}