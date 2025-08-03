namespace Infrastructure.Configuration;

public static class DiscordRegistrar {
    public static void AddDiscord(this IServiceCollection services) {
        services.AddSingleton<DiscordMultiplexer>();
        services.AddSingleton<IDiscordService>(provider => provider.GetRequiredService<DiscordMultiplexer>());
    }

    public static async Task UseDiscordAsync(this IServiceProvider provider) {
        using var scope = provider.CreateScope();
        var multiplexer = scope.ServiceProvider.GetRequiredService<DiscordMultiplexer>();
        await multiplexer.StartAsync();
    }
}