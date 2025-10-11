namespace Infrastructure.Configuration;

public static class S3HandlerRegistrar {
    public static void AddS3Handlers(this IServiceCollection services) {
        services.AddSingleton<IS3SettingsHandler, S3SettingsHandler>();
        services.AddSingleton<IS3AuthHandler, S3AuthHandler>();
        services.AddSingleton<IS3ObjectHandler, S3ObjectHandler>();
        services.AddSingleton<IS3BucketHandler, S3BucketHandler>();
        services.AddSingleton<IBucketStore, DiscordBucketStore>();
        services.AddSingleton<IHealthStatusService, HealthStatusService>();
        services.AddSingleton<IObjectEncryptionService, ObjectEncryptionService>();

        services.AddSingleton(TimeProvider.System);
    }
}