using Microsoft.Extensions.Hosting;

namespace WebApi;

public class S3Manager {
    private readonly ILogger<S3Manager> _logger;
    private readonly S3ServerSettings _s3Settings;
    private readonly IS3SettingsHandler _s3SettingsHandler;
    private readonly IS3ObjectHandler _s3ObjectHandler;
    private readonly IS3BucketHandler _s3BucketHandler;
    private readonly IHostApplicationLifetime _appLifetime;

    public S3Manager(ILogger<S3Manager> logger, S3ServerSettings s3Settings, IS3SettingsHandler s3SettingsHandler,
        IS3ObjectHandler s3ObjectHandler, IS3BucketHandler s3BucketHandler, IHostApplicationLifetime appLifetime) {
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
        server.Object.ReadTagging = _s3ObjectHandler.ReadTags;
        server.Object.WriteTagging = _s3ObjectHandler.WriteTags;
        server.Object.DeleteTagging = _s3ObjectHandler.DeleteTags;

        server.Object.CreateMultipartUpload = _s3ObjectHandler.CreateMultipartUpload;
        server.Object.UploadPart = _s3ObjectHandler.UploadPart;
        server.Object.ReadParts = _s3ObjectHandler.ReadParts;
        server.Object.CompleteMultipartUpload = _s3ObjectHandler.CompleteMultipartUpload;
        server.Object.AbortMultipartUpload = _s3ObjectHandler.AbortMultipartUpload;

        server.Bucket.Write = _s3BucketHandler.Write;
        server.Bucket.Read = _s3BucketHandler.Read;
        server.Bucket.Delete = _s3BucketHandler.Delete;
        server.Bucket.ReadTagging = _s3BucketHandler.ReadTags;
        server.Bucket.WriteTagging = _s3BucketHandler.WriteTags;
        server.Bucket.DeleteTagging = _s3BucketHandler.DeleteTags;
        server.Bucket.Exists = _s3BucketHandler.Exists;
        server.Bucket.ReadVersions = _s3BucketHandler.ReadVersions;
        server.Bucket.ReadAcl = _s3BucketHandler.ReadAcl;

        server.Service.ListBuckets = _s3BucketHandler.ListBuckets;


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
        if (str.Contains("exception", StringComparison.OrdinalIgnoreCase) ||
            str.Contains("error", StringComparison.OrdinalIgnoreCase)) {
            _logger.LogError(str);
        }
        else if (str.Contains("warning", StringComparison.OrdinalIgnoreCase)) {
            _logger.LogWarning(str);
        }
    }
}