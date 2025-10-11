using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Infrastructure.Telemetry;

public sealed class StorageMetricsWarmup : IHostedService {
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly StorageMetrics _metrics;
    private readonly ILogger<StorageMetricsWarmup> _logger;

    public StorageMetricsWarmup(IServiceScopeFactory scopeFactory, StorageMetrics metrics, ILogger<StorageMetricsWarmup> logger) {
        _scopeFactory = scopeFactory;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken) {
        try {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<IDbContext>();

            var bucketCount = await db.Buckets
                .AsNoTracking()
                .CountAsync(cancellationToken);

            var objectQuery = db.Objects
                .AsNoTracking()
                .Where(o => !o.DeleteMarker);

            var objectCount = await objectQuery.CountAsync(cancellationToken);

            var dataUsage = await objectQuery
                .SumAsync(o => o.StorageContentLength > 0 ? o.StorageContentLength : o.ContentLength, cancellationToken);

            _metrics.Initialize(bucketCount, objectCount, dataUsage);
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to warm up storage metrics");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
