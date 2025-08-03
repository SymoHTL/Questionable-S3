using Microsoft.Extensions.Hosting;
using System.Threading;

namespace WebApi;

public class S3Manager {
    private readonly ILogger<S3Manager> _logger;
    private readonly S3ServerSettings _s3Settings;
    private readonly IS3SettingsHandler _s3SettingsHandler;
    private readonly IS3ObjectHandler  _s3ObjectHandler;
    private readonly IS3BucketHandler  _s3BucketHandler;
    private readonly IHostApplicationLifetime _appLifetime;

    public S3Manager(ILogger<S3Manager> logger, S3ServerSettings s3Settings, IS3SettingsHandler s3SettingsHandler, IS3ObjectHandler s3ObjectHandler, IS3BucketHandler s3BucketHandler, IHostApplicationLifetime appLifetime) {
        _logger = logger;
        _s3Settings = s3Settings;
        _s3SettingsHandler = s3SettingsHandler;
        _s3ObjectHandler = s3ObjectHandler;
        _s3BucketHandler = s3BucketHandler;
        _appLifetime = appLifetime;
    }

    public async Task Run() {
        _logger.LogInformation("Configuring S3 Server");
        using var server = new S3Server(_s3Settings);
        server.Settings.Logger = Log;
        
        server.Settings.PreRequestHandler = _s3SettingsHandler.PreRequestHandler;
        server.Settings.PostRequestHandler = _s3SettingsHandler.PostRequestHandler;
        server.Settings.DefaultRequestHandler = _s3SettingsHandler.DefaultRequestHandler;
        
        server.Object.Read = _s3ObjectHandler.Read;
        server.Object.Write = _s3ObjectHandler.Write;
        server.Object.Delete = _s3ObjectHandler.Delete;
        
        server.Bucket.Write = _s3BucketHandler.Write;

        _logger.LogInformation("Starting S3 Server, listening on {Endpoint}", _s3Settings.Webserver.Prefix);
        server.Start();

        var tcs = new TaskCompletionSource();
        _appLifetime.ApplicationStopping.Register(() => tcs.TrySetResult());
        await tcs.Task;

        _logger.LogInformation("Stopping S3 Server");
        server.Stop();
        _logger.LogInformation("S3 Server stopped");
    }

    private void Log(string str) {
        if (str.Contains("exception", StringComparison.OrdinalIgnoreCase) || str.Contains("error", StringComparison.OrdinalIgnoreCase)) {
            _logger.LogError(str);
        } else if (str.Contains("warning", StringComparison.OrdinalIgnoreCase)) {
            _logger.LogWarning(str);
        }
    }
}