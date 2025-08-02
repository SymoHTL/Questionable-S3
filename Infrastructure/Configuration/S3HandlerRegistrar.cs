using Infrastructure.S3Handlers.Settings;

namespace Infrastructure.Configuration;

public static class S3HandlerRegistrar {
    public static void AddS3Handlers(this IServiceCollection services) {
        services.AddSingleton<S3SettingsHandler>();
    }
}