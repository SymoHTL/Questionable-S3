using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Infrastructure.Telemetry;

public sealed class StorageMetrics : IStorageMetrics, IDisposable {
    public const string MeterName = "QuestionableS3.Storage";

    private readonly Counter<long> _ingressBytes;
    private readonly Counter<long> _egressBytes;
    private readonly UpDownCounter<long> _dataUsageBytes;
    private readonly UpDownCounter<long> _bucketCount;
    private readonly UpDownCounter<long> _objectCount;
    private readonly Counter<long> _discordRequestCount;
    private readonly Counter<long> _discordRefreshRequestCount;
    private readonly ObservableGauge<double> _discordRequestsPerSecond;
    private readonly ObservableGauge<long> _totalStoredBytesGauge;
    private readonly ILogger<StorageMetrics> _logger;
    private readonly Meter _meter;
    private readonly Queue<DateTimeOffset> _discordRequestTimestamps = new();
    private readonly TimeSpan _discordRequestWindow = TimeSpan.FromSeconds(60);
    private readonly TimeProvider _timeProvider;
    private readonly object _sync = new();

    private long _currentBucketCount;
    private long _currentObjectCount;
    private long _currentDataUsage;
    private bool _initialized;

    public StorageMetrics(IMeterFactory meterFactory, ILogger<StorageMetrics> logger, TimeProvider timeProvider) {
        ArgumentNullException.ThrowIfNull(meterFactory);
        _logger = logger;
        _timeProvider = timeProvider;

        _meter = meterFactory.Create(MeterName);

        _ingressBytes = _meter.CreateCounter<long>(
            name: "questionable_s3_total_ingress_bytes",
            unit: "By",
            description: "Total bytes uploaded to the Questionable S3 storage service."
        );

        _egressBytes = _meter.CreateCounter<long>(
            name: "questionable_s3_total_egress_bytes",
            unit: "By",
            description: "Total bytes downloaded from the Questionable S3 storage service."
        );

        _dataUsageBytes = _meter.CreateUpDownCounter<long>(
            name: "questionable_s3_data_usage_bytes",
            unit: "By",
            description: "Net data volume currently stored across all Questionable S3 objects."
        );

        _bucketCount = _meter.CreateUpDownCounter<long>(
            name: "questionable_s3_bucket_count",
            description: "Total number of buckets managed by the Questionable S3 service."
        );

        _objectCount = _meter.CreateUpDownCounter<long>(
            name: "questionable_s3_object_count",
            description: "Total number of stored object versions within the Questionable S3 service."
        );

        _discordRequestCount = _meter.CreateCounter<long>(
            name: "questionable_s3_discord_request_total",
            description: "Total number of requests issued to Discord services." 
        );

        _discordRefreshRequestCount = _meter.CreateCounter<long>(
            name: "questionable_s3_discord_refresh_request_total",
            description: "Total number of Discord attachment refresh requests." 
        );

        _discordRequestsPerSecond = _meter.CreateObservableGauge<double>(
            name: "questionable_s3_discord_requests_per_second",
            observeValues: ObserveDiscordRequestsPerSecond,
            unit: "1/s",
            description: "Smoothed average of Discord requests per second over the last minute."
        );

        _totalStoredBytesGauge = _meter.CreateObservableGauge<long>(
            name: "questionable_s3_total_stored_bytes",
            observeValues: ObserveTotalStoredBytes,
            unit: "By",
            description: "Current total bytes stored across all Questionable S3 objects."
        );
    }

    public void Initialize(long bucketCount, long objectCount, long dataUsageBytes) {
        lock (_sync) {
            var bucketDelta = bucketCount - _currentBucketCount;
            if (bucketDelta != 0) {
                _bucketCount.Add(bucketDelta);
                _currentBucketCount += bucketDelta;
            }

            var objectDelta = objectCount - _currentObjectCount;
            if (objectDelta != 0) {
                _objectCount.Add(objectDelta);
                _currentObjectCount += objectDelta;
            }

            var usageDelta = dataUsageBytes - _currentDataUsage;
            if (usageDelta != 0) {
                _dataUsageBytes.Add(usageDelta);
                _currentDataUsage += usageDelta;
            }

            if (!_initialized) {
                _initialized = true;
                _logger.LogInformation("Storage metrics initialized with {BucketCount} buckets, {ObjectCount} objects, and {DataUsage} bytes", bucketCount, objectCount, dataUsageBytes);
            }
        }
    }

    public void RecordIngress(long bytes) {
        if (bytes <= 0) return;
        _ingressBytes.Add(bytes);
    }

    public void RecordEgress(long bytes) {
        if (bytes <= 0) return;
        _egressBytes.Add(bytes);
    }

    public void TrackObjectStored(long storageBytes) {
        if (storageBytes < 0) storageBytes = 0;

        lock (_sync) {
            _objectCount.Add(1);
            _currentObjectCount += 1;

            if (storageBytes > 0) {
                _dataUsageBytes.Add(storageBytes);
                _currentDataUsage += storageBytes;
            }
        }
    }

    public void TrackObjectDeleted(long storageBytes) {
        if (storageBytes < 0) storageBytes = 0;

        lock (_sync) {
            if (_currentObjectCount > 0) {
                _objectCount.Add(-1);
                _currentObjectCount -= 1;
            }

            if (storageBytes > 0) {
                var delta = Math.Min(storageBytes, _currentDataUsage);
                _dataUsageBytes.Add(-delta);
                _currentDataUsage -= delta;
            }
        }
    }

    public void TrackBucketCreated() {
        lock (_sync) {
            _bucketCount.Add(1);
            _currentBucketCount += 1;
        }
    }

    public void TrackBucketDeleted() {
        lock (_sync) {
            if (_currentBucketCount > 0) {
                _bucketCount.Add(-1);
                _currentBucketCount -= 1;
            }
        }
    }

    public void RecordDiscordRequest() {
        lock (_sync) {
            _discordRequestCount.Add(1);
            var now = _timeProvider.GetUtcNow();
            _discordRequestTimestamps.Enqueue(now);
            TrimDiscordRequestWindow(now);
        }
    }

    public void RecordDiscordRefreshRequest() {
        _discordRefreshRequestCount.Add(1);
    }

    public void Dispose() {
        _meter.Dispose();
    }

    private void TrimDiscordRequestWindow(DateTimeOffset now) {
        while (_discordRequestTimestamps.Count > 0 && now - _discordRequestTimestamps.Peek() > _discordRequestWindow) {
            _discordRequestTimestamps.Dequeue();
        }
    }

    private IEnumerable<Measurement<double>> ObserveDiscordRequestsPerSecond() {
        lock (_sync) {
            var now = _timeProvider.GetUtcNow();
            TrimDiscordRequestWindow(now);
            var rate = _discordRequestTimestamps.Count / _discordRequestWindow.TotalSeconds;
            return new[] { new Measurement<double>(rate) };
        }
    }

    private IEnumerable<Measurement<long>> ObserveTotalStoredBytes() {
        lock (_sync) {
            return new[] { new Measurement<long>(_currentDataUsage) };
        }
    }
}
