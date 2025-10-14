using System.Linq;
using System.Net;
using System.Text.Json;
using S3ServerLibrary.S3Objects;

namespace Application.Services.S3Handlers.Settings;

public class S3SettingsHandler : IS3SettingsHandler {
    private readonly IS3AuthHandler _auth;
    private readonly ILogger<HttpRequestHandler> _httpLogger;
    private readonly IHealthStatusService _healthStatusService;
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    // ReSharper disable once ContextualLoggerProblem
    public S3SettingsHandler(ILogger<HttpRequestHandler> httpLogger,
        IS3AuthHandler auth, IHealthStatusService healthStatusService) {
        _httpLogger = httpLogger;
        _auth = auth;
        _healthStatusService = healthStatusService;
    }


    public async Task<bool> PreRequestHandler(S3Context ctx) {
        if (_httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Request: {Route} {Method}",
                ctx.Http.Request.Url.Full, ctx.Http.Request.Method);

        if (ctx.Http.Request.Url.Elements.Length == 1) {
            var segment = ctx.Http.Request.Url.Elements[0];

            if (segment.Equals("robots.txt", StringComparison.OrdinalIgnoreCase)) {
                ctx.Response.ContentType = "text/plain";
                ctx.Response.StatusCode = (int)HttpStatusCode.OK;
                await ctx.Response.Send("User-Agent: *\r\nDisallow:\r\n");
                return true;
            }

            if (await TryHandleHealthAsync(ctx, segment))
                return true;
        }

        var md = await _auth.AuthenticateAndBuildMetadataAsync(ctx);

        switch (ctx.Request.RequestType) {
            case S3RequestType.ListBuckets:
                md = _auth.AuthorizeServiceRequest(ctx, md);
                break;

            case S3RequestType.BucketDelete:
            case S3RequestType.BucketDeleteTags:
            case S3RequestType.BucketDeleteWebsite:
            case S3RequestType.BucketExists:
            case S3RequestType.BucketRead:
            case S3RequestType.BucketReadAcl:
            case S3RequestType.BucketReadLocation:
            case S3RequestType.BucketReadLogging:
            case S3RequestType.BucketReadTags:
            case S3RequestType.BucketReadVersioning:
            case S3RequestType.BucketReadVersions:
            case S3RequestType.BucketReadWebsite:
            case S3RequestType.BucketWrite:
            case S3RequestType.BucketWriteAcl:
            case S3RequestType.BucketWriteLogging:
            case S3RequestType.BucketWriteTags:
            case S3RequestType.BucketWriteVersioning:
            case S3RequestType.BucketWriteWebsite:
                md = _auth.AuthorizeBucketRequest(ctx, md);
                break;

            case S3RequestType.ObjectDelete:
            case S3RequestType.ObjectDeleteMultiple:
            case S3RequestType.ObjectDeleteTags:
            case S3RequestType.ObjectExists:
            case S3RequestType.ObjectRead:
            case S3RequestType.ObjectReadAcl:
            case S3RequestType.ObjectReadLegalHold:
            case S3RequestType.ObjectReadRange:
            case S3RequestType.ObjectReadRetention:
            case S3RequestType.ObjectReadTags:
            case S3RequestType.ObjectReadParts:
            case S3RequestType.ObjectWrite:
            case S3RequestType.ObjectWriteAcl:
            case S3RequestType.ObjectWriteLegalHold:
            case S3RequestType.ObjectWriteRetention:
            case S3RequestType.ObjectWriteTags:
            case S3RequestType.ObjectCreateMultipartUpload:
            case S3RequestType.ObjectUploadPart:
            case S3RequestType.ObjectCompleteMultipartUpload:
            case S3RequestType.ObjectAbortMultipartUpload:
                md = _auth.AuthorizeObjectRequest(ctx, md);
                break;
        }

        ctx.Metadata = md;


        if (ctx.Http.Request.Query.Elements is not null &&
            ctx.Http.Request.Query.Elements.AllKeys.Contains("metadata")) {
            ctx.Response.ContentType = "application/json";
            await ctx.Response.Send(SerializationHelper.SerializeJson(md));
            return true;
        }

        return false;
    }

    public Task PostRequestHandler(S3Context ctx) {
        if (_httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Response: {Route} {Method} - {StatusCode}",
                ctx.Http.Request.Url.Full, ctx.Http.Request.Method, ctx.Response.StatusCode);
        return Task.CompletedTask;
    }

    public async Task DefaultRequestHandler(S3Context ctx) {
        _httpLogger.LogWarning("Default request handler invoked for {Route} - {Method} - {Type}",
            ctx.Http.Request.Url.Full, ctx.Http.Request.Method, ctx.Request.RequestType);
        await ctx.Response.Send(ErrorCode.InvalidRequest);
    }


    public abstract class HttpRequestHandler;

    private async Task<bool> TryHandleHealthAsync(S3Context ctx, string segment) {
        if (!segment.Equals("health", StringComparison.OrdinalIgnoreCase) &&
            !segment.Equals("ready", StringComparison.OrdinalIgnoreCase))
            return false;

        var method = ctx.Http.Request.Method.ToString();
        var isHead = string.Equals(method, "HEAD", StringComparison.OrdinalIgnoreCase);
        if (!isHead && !string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase)) {
            ctx.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
            await ctx.Response.Send(string.Empty);
            return true;
        }

        var report = segment.Equals("ready", StringComparison.OrdinalIgnoreCase)
            ? await _healthStatusService.GetReadinessAsync(ctx.Http.Token)
            : await _healthStatusService.GetLivenessAsync(ctx.Http.Token);

        ctx.Response.ContentType = "application/json";
        ctx.Response.StatusCode = report.Healthy ? (int)HttpStatusCode.OK : (int)HttpStatusCode.ServiceUnavailable;

        if (!isHead) {
            var json = JsonSerializer.Serialize(new {
                report.Status,
                report.Healthy,
                report.Timestamp,
                Checks = report.Checks.Select(c => new { c.Name, c.Healthy, c.Error })
            }, JsonOptions);
            await ctx.Response.Send(json);
        }
        else {
            await ctx.Response.Send(string.Empty);
        }

        return true;
    }
}