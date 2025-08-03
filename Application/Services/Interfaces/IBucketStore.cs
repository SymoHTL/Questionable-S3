namespace Application.Services.Interfaces;

public interface IBucketStore {
    Task<bool> AddObjectAsync(string filePath, Bucket bucket, Object obj, CancellationToken ct);
    Task<Stream> GetObjectStreamAsync(Bucket bucket, Object obj, CancellationToken ct);
}