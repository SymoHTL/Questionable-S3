using Infrastructure.S3Handlers.Settings;
using S3ServerLibrary;

namespace WebApi;

public class S3Manager {
    private readonly ILogger<S3Manager> _logger;
    private readonly S3ServerSettings _s3Settings;
    private readonly S3SettingsHandler _s3SettingsHandler;

    public S3Manager(ILogger<S3Manager> logger, S3ServerSettings s3Settings, S3SettingsHandler s3SettingsHandler) {
        _logger = logger;
        _s3Settings = s3Settings;
        _s3SettingsHandler = s3SettingsHandler;
    }

    public void Run() {
        _logger.LogInformation("Configuring S3 Server");
        using var server = new S3Server(_s3Settings);
        server.Settings.PreRequestHandler = _s3SettingsHandler.PreRequestHandler;
        server.Settings.PostRequestHandler = _s3SettingsHandler.PostRequestHandler;
        server.Settings.DefaultRequestHandler = _s3SettingsHandler.DefaultRequestHandler;
        
        _logger.LogInformation("Starting S3 Server, listening on {Endpoint}", _s3Settings.Webserver.Prefix);
        server.Start();

        EventWaitHandle waitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, null);
        var waitHandleSignal = false;
        do {
            waitHandleSignal = waitHandle.WaitOne(1000);
        } while (!waitHandleSignal);

        _logger.LogInformation("Stopping S3 Server");
        server.Stop();
        _logger.LogInformation("S3 Server stopped");
    }

    
}