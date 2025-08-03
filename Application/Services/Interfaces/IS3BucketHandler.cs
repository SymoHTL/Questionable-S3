namespace Application.Services.Interfaces;

public interface IS3BucketHandler {
    Task Write(S3Context ctx);
}