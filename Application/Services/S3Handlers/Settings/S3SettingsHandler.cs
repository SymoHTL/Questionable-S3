using S3ServerLibrary.S3Objects;
using HttpMethod = WatsonWebserver.Core.HttpMethod;

namespace Application.Services.S3Handlers.Settings;

public abstract class S3SettingsHandler : IS3SettingsHandler {
    private readonly IS3AuthHandler _auth;
    private readonly ILogger<HttpRequestHandler> _httpLogger;
    private readonly S3ServerSettings _s3Settings;

    // ReSharper disable once ContextualLoggerProblem
    protected S3SettingsHandler(S3ServerSettings s3Settings, ILogger<HttpRequestHandler> httpLogger,
        IS3AuthHandler auth) {
        _s3Settings = s3Settings;
        _httpLogger = httpLogger;
        _auth = auth;
    }


    public async Task<bool> PreRequestHandler(S3Context ctx) {
        if (_s3Settings.Logging.HttpRequests && _httpLogger.IsEnabled(LogLevel.Information))
            _httpLogger.LogInformation("Request: {@Request}", ctx.Http.Request);


        if (ctx.Http.Request.Url.Elements.Length == 1)
            // TODO: favicons, etc.
            if (ctx.Http.Request.Url.Elements[0].Equals("robots.txt")) {
                ctx.Response.ContentType = "text/plain";
                ctx.Response.StatusCode = 200;
                await ctx.Response.Send("User-Agent: *\r\nDisallow:\r\n");
                return true;
            }


        if (!ctx.Http.Request.Headers.AllKeys.Contains("Authorization"))
            if (ctx.Http.Request.Method == HttpMethod.GET)
                if (ctx.Http.Request.Url.Elements == null || ctx.Http.Request.Url.Elements.Length < 1) {
                    ctx.Response.StatusCode = 200;
                    ctx.Response.ContentType = "text/html";
                    await ctx.Response.Send(DefaultPage("https://github.com/jchristn/less3"));
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
            _httpLogger.LogInformation("Default request handler invoked for {@Request}", ctx.Http.Request);
        await ctx.Response.Send(ErrorCode.InvalidRequest);
    }

    private static string DefaultPage(string link) {
        var html =
            "<html>" + Environment.NewLine +
            "   <head>" + Environment.NewLine +
            "      <title>&lt;3 :: Less3 :: S3-Compatible Object Storage</title>" + Environment.NewLine +
            "      <style>" + Environment.NewLine +
            "          body {" + Environment.NewLine +
            "            font-family: arial;" + Environment.NewLine +
            "          }" + Environment.NewLine +
            "          pre {" + Environment.NewLine +
            "            background-color: #e5e7ea;" + Environment.NewLine +
            "            color: #333333; " + Environment.NewLine +
            "          }" + Environment.NewLine +
            "          h3 {" + Environment.NewLine +
            "            color: #333333; " + Environment.NewLine +
            "            padding: 4px;" + Environment.NewLine +
            "            border: 4px;" + Environment.NewLine +
            "          }" + Environment.NewLine +
            "          p {" + Environment.NewLine +
            "            color: #333333; " + Environment.NewLine +
            "            padding: 4px;" + Environment.NewLine +
            "            border: 4px;" + Environment.NewLine +
            "          }" + Environment.NewLine +
            "          a {" + Environment.NewLine +
            "            background-color: #4cc468;" + Environment.NewLine +
            "            color: white;" + Environment.NewLine +
            "            padding: 4px;" + Environment.NewLine +
            "            border: 4px;" + Environment.NewLine +
            "         text-decoration: none; " + Environment.NewLine +
            "          }" + Environment.NewLine +
            "          li {" + Environment.NewLine +
            "            padding: 6px;" + Environment.NewLine +
            "            border: 6px;" + Environment.NewLine +
            "          }" + Environment.NewLine +
            "      </style>" + Environment.NewLine +
            "   </head>" + Environment.NewLine +
            "   <body>" + Environment.NewLine +
            "      <pre>" + Environment.NewLine +
            WebUtility.HtmlEncode(LogoPlain()) +
            "      </pre>" + Environment.NewLine +
            "      <p>Congratulations, your Less3 node is running!</p>" + Environment.NewLine +
            "      <p>" + Environment.NewLine +
            "        <a href='" + link + "' target='_blank'>Source Code</a>" + Environment.NewLine +
            "      </p>" + Environment.NewLine +
            "   </body>" + Environment.NewLine +
            "</html>";

        return html;
    }

    private static string LogoPlain() {
        // http://loveascii.com/hearts.html
        // http://patorjk.com/software/taag/#p=display&f=Small&t=less3 

        var ret = Environment.NewLine;
        ret +=
            "  ,d88b.d88b,  " + @"  _           ____  " + Environment.NewLine +
            "  88888888888  " + @" | |___ _____|__ /  " + Environment.NewLine +
            "  `Y8888888Y'  " + @" | / -_|_-<_-<|_ \  " + Environment.NewLine +
            "    `Y888Y'    " + @" |_\___/__/__/___/  " + Environment.NewLine +
            "      `Y'      " + Environment.NewLine;

        return ret;
    }

    public abstract class HttpRequestHandler;
}