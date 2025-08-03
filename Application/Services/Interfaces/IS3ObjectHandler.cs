namespace Application.Services.Interfaces;

public interface IS3ObjectHandler {
    Task<S3Object> Read(S3Context ctx);
    Task Write(S3Context ctx);
    Task Delete(S3Context ctx);
}