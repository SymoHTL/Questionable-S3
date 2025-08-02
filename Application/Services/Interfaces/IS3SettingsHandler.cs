namespace Application.Services.Interfaces;

public interface IS3SettingsHandler {
    Task<bool> PreRequestHandler(S3Context ctx);
    Task PostRequestHandler(S3Context ctx);
    Task DefaultRequestHandler(S3Context ctx);
}