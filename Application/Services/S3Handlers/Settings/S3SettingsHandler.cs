using S3ServerLibrary.S3Objects;

namespace Application.Services.S3Handlers.Settings;

public class S3SettingsHandler : IS3SettingsHandler {
    private readonly IS3AuthHandler _auth;
    private readonly ILogger<HttpRequestHandler> _httpLogger;
    private readonly S3ServerSettings _s3Settings;

    // ReSharper disable once ContextualLoggerProblem
    public S3SettingsHandler(S3ServerSettings s3Settings, ILogger<HttpRequestHandler> httpLogger,
        IS3AuthHandler auth) {
        _s3Settings = s3Settings;
        _httpLogger = httpLogger;
        _auth = auth;
    }


    public async Task<bool> PreRequestHandler(S3Context ctx) {
        if (_s3Settings.Logging.HttpRequests && _httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Request: {Route} {Method} from {Source}",
                ctx.Http.Request.Url.Full, ctx.Http.Request.Method, ctx.Http.Request.Source);


        if (ctx.Http.Request.Url.Elements.Length == 1)
            // TODO: favicons, etc.
            if (ctx.Http.Request.Url.Elements[0].Equals("robots.txt")) {
                ctx.Response.ContentType = "text/plain";
                ctx.Response.StatusCode = 200;
                await ctx.Response.Send("User-Agent: *\r\nDisallow:\r\n");
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
            case S3RequestType.ObjectWrite:
            case S3RequestType.ObjectWriteAcl:
            case S3RequestType.ObjectWriteLegalHold:
            case S3RequestType.ObjectWriteRetention:
            case S3RequestType.ObjectWriteTags:
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
        if (_s3Settings.Logging.HttpRequests && _httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Response: {@Response}", ctx.Response);
        return Task.CompletedTask;
    }

    public async Task DefaultRequestHandler(S3Context ctx) {
        if (_httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Default request handler invoked for {Route}", ctx.Http.Request.Url.Full);
        await ctx.Response.Send(ErrorCode.InvalidRequest);
    }


    public abstract class HttpRequestHandler;
}