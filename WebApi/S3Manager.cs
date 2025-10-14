using System.ComponentModel;
using Microsoft.Extensions.Hosting;
using S3ServerLibrary.S3Objects;

namespace WebApi;

public class S3Manager {
    private readonly ILogger<S3Manager> _logger;
    private readonly S3ServerSettings _s3Settings;
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly IServiceScopeFactory _scopeFactory;

    public S3Manager(ILogger<S3Manager> logger, S3ServerSettings s3Settings, IHostApplicationLifetime appLifetime,
        IServiceScopeFactory scopeFactory) {
        _logger = logger;
        _s3Settings = s3Settings;
        _appLifetime = appLifetime;
        _scopeFactory = scopeFactory;
    }

    public async Task Run() {
        _logger.LogInformation("Configuring S3 Server");
        using var server = new S3Server(_s3Settings);
        server.Settings.Logger = Log;

        server.Settings.PreRequestHandler = PreRequestHandler;
        server.Settings.PostRequestHandler = PostRequestHandler;
        server.Settings.DefaultRequestHandler = DefaultRequestHandler;

        server.Object.Read = ObjectRead;
        server.Object.Write = ObjectWrite;
        server.Object.Delete = ObjectDelete;
        server.Object.ReadTagging = ObjectReadTagging;
        server.Object.WriteTagging = ObjectWriteTagging;
        server.Object.DeleteTagging = ObjectDeleteTagging;

        server.Object.CreateMultipartUpload = ObjectCreateMultipartUpload;
        server.Object.UploadPart = ObjectUploadPart;
        server.Object.ReadParts = ObjectReadParts;
        server.Object.CompleteMultipartUpload = ObjectCompleteMultipartUpload;
        server.Object.AbortMultipartUpload = ObjectAbortMultipartUpload;

        server.Bucket.Write = BucketWrite;
        server.Bucket.Read = BucketRead;
        server.Bucket.Delete = BucketDelete;
        server.Bucket.ReadTagging = BucketReadTagging;
        server.Bucket.WriteTagging = BucketWriteTagging;
        server.Bucket.DeleteTagging = BucketDeleteTagging;
        server.Bucket.Exists = BucketExists;
        server.Bucket.ReadVersions = BucketVersions;
        server.Bucket.ReadAcl = BucketReadAcl;

        server.Service.ListBuckets = BucketList;


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

    private async Task<bool> PreRequestHandler(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3SettingsHandler>();
        return await handler.PreRequestHandler(ctx);
    }

    private async Task PostRequestHandler(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3SettingsHandler>();
        await handler.PostRequestHandler(ctx);
    }

    private async Task DefaultRequestHandler(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3SettingsHandler>();
        await handler.DefaultRequestHandler(ctx);
    }

    private async Task<S3Object> ObjectRead(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        return await handler.Read(ctx);
    }

    private async Task ObjectWrite(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.Write(ctx);
    }

    private async Task ObjectDelete(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.Delete(ctx);
    }

    private async Task<Tagging> ObjectReadTagging(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        return await handler.ReadTags(ctx);
    }

    private async Task ObjectWriteTagging(S3Context ctx, Tagging tagging) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.WriteTags(ctx, tagging);
    }

    private async Task ObjectDeleteTagging(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.DeleteTags(ctx);
    }

    private async Task<InitiateMultipartUploadResult> ObjectCreateMultipartUpload(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        return await handler.CreateMultipartUpload(ctx);
    }

    private async Task ObjectUploadPart(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.UploadPart(ctx);
    }

    private async Task<ListPartsResult> ObjectReadParts(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        return await handler.ReadParts(ctx);
    }

    private async Task<CompleteMultipartUploadResult> ObjectCompleteMultipartUpload(S3Context ctx,
        CompleteMultipartUpload upload) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        return await handler.CompleteMultipartUpload(ctx, upload);
    }

    private async Task ObjectAbortMultipartUpload(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3ObjectHandler>();
        await handler.AbortMultipartUpload(ctx);
    }

    private async Task BucketWrite(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        await handler.Write(ctx);
    }

    private async Task<ListBucketResult> BucketRead(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.Read(ctx);
    }

    private async Task BucketDelete(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        await handler.Delete(ctx);
    }

    private async Task<Tagging> BucketReadTagging(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.ReadTags(ctx);
    }

    private async Task BucketWriteTagging(S3Context ctx, Tagging tagging) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        await handler.WriteTags(ctx, tagging);
    }

    private async Task BucketDeleteTagging(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        await handler.DeleteTags(ctx);
    }

    private async Task<bool> BucketExists(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.Exists(ctx);
    }

    private async Task<ListVersionsResult> BucketVersions(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.ReadVersions(ctx);
    }

    private async Task<AccessControlPolicy> BucketReadAcl(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.ReadAcl(ctx);
    }

    private async Task<ListAllMyBucketsResult> BucketList(S3Context ctx) {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<IS3BucketHandler>();
        return await handler.ListBuckets(ctx);
    }
}