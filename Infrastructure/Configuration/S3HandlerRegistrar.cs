namespace Infrastructure.Configuration;

public static class S3HandlerRegistrar {
    public static void AddS3Handlers(this IServiceCollection services) {
        services.AddSingleton(TimeProvider.System);

        services.AddSingleton<IS3SettingsHandler, S3SettingsHandler>();
        services.AddSingleton<IS3AuthHandler, S3AuthHandler>();

        services.AddSingleton<StorageMetrics>();
        services.AddSingleton<IStorageMetrics>(p => p.GetRequiredService<StorageMetrics>());
        services.AddHostedService<StorageMetricsWarmup>();

        services.AddSingleton<IS3ObjectHandler, S3ObjectHandler>();
        services.AddSingleton<IS3BucketHandler, S3BucketHandler>();
        services.AddSingleton<IBucketStore, DiscordBucketStore>();
        services.AddSingleton<IHealthStatusService, HealthStatusService>();
        services.AddSingleton<IObjectEncryptionService, ObjectEncryptionService>();
    }
}