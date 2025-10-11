namespace Application.Services.Interfaces;

public interface IStorageMetrics {
    void RecordIngress(long bytes);
    void RecordEgress(long bytes);
    void TrackObjectStored(long storageBytes);
    void TrackObjectDeleted(long storageBytes);
    void TrackBucketCreated();
    void TrackBucketDeleted();
    void Initialize(long bucketCount, long objectCount, long dataUsageBytes);
    void RecordDiscordRequest();
    void RecordDiscordRefreshRequest();
}
