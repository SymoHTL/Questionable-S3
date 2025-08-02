using Infrastructure.Configuration;

var builder = CoconaApp.CreateBuilder();

builder.Configuration.AddJsonFile("appsettings.json", false, true);
builder.Configuration.AddEnvironmentVariables();

builder.Logging.ClearProviders();
builder.Logging.AddConfiguration(builder.Configuration.GetSection("Logging"));
builder.Logging.AddConsole();

builder.Services.AddDatabase(builder.Configuration);
builder.Services.AddS3Settings(builder.Configuration);
builder.Services.AddS3Handlers();


var app = builder.Build();

await app.Services.UseDatabaseAsync();


app.Run<S3Manager>();