using System.Net.Http.Headers;
using Domain.Common;

var builder = CoconaApp.CreateBuilder();

builder.Configuration.AddJsonFile("appsettings.json", false, true);
builder.Configuration.AddJsonFile("appsettings.Development.json", true, true);
builder.Configuration.AddEnvironmentVariables();

builder.Logging.ClearProviders();
builder.Logging.AddConfiguration(builder.Configuration.GetSection("Logging"));
builder.Logging.AddConsole();

builder.Services.AddHttpClient(Constants.HttpClients.Discord, client => {
    client.Timeout = TimeSpan.FromMinutes(1);
    client.DefaultRequestVersion = new Version(2, 0);
    client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0");
});

builder.Services.AddDatabase(builder.Configuration);
builder.Services.AddS3Settings(builder.Configuration);
builder.Services.AddS3Handlers();
builder.Services.AddDiscord();
builder.Services.AddHangfire(builder.Configuration);
builder.Services.AddTelemetry(builder.Configuration, builder.Logging);

builder.Services.AddSingleton<S3Manager>();


var app = builder.Build();

await app.Services.UseDatabaseAsync(app.Configuration);
await app.Services.UseDiscordAsync();

app.AddCommands<HealthCommands>();
app.AddCommand(async (S3Manager manager) => await manager.Run());

await app.RunAsync();