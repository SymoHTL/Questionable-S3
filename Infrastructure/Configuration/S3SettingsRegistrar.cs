namespace Infrastructure.Configuration;

public static class S3SettingsRegistrar {
    public static void AddS3Settings(this IServiceCollection services, IConfiguration configuration) {
        var s3Settings = configuration.GetSection("S3Settings").Get<S3ServerSettings>() ??
                          throw new InvalidOperationException("S3Settings section is missing or invalid in the configuration.");
        
        services.AddSingleton(s3Settings);
    }
}