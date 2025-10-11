using Application.Services.Health;
using Application.Services.Interfaces;
using Infrastructure.Configuration;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

namespace Infrastructure.Services;

public class HealthStatusService : IHealthStatusService {
    private readonly IDbContextFactory<QuestionableDbContext> _dbContextFactory;
    private readonly DiscordMultiplexer _discordMultiplexer;
    private readonly IConfiguration _configuration;
    private readonly ILogger<HealthStatusService> _logger;
    private readonly TimeProvider _timeProvider;

    public HealthStatusService(
        IDbContextFactory<QuestionableDbContext> dbContextFactory,
        DiscordMultiplexer discordMultiplexer,
        IConfiguration configuration,
        ILogger<HealthStatusService> logger,
        TimeProvider timeProvider) {
        _dbContextFactory = dbContextFactory;
        _discordMultiplexer = discordMultiplexer;
        _configuration = configuration;
        _logger = logger;
        _timeProvider = timeProvider;
    }

    public Task<HealthStatusReport> GetLivenessAsync(CancellationToken cancellationToken = default) {
        var checks = new List<HealthCheckResult> { new("process", true, null) };
        return Task.FromResult(BuildReport(checks));
    }

    public async Task<HealthStatusReport> GetReadinessAsync(CancellationToken cancellationToken = default) {
        var checks = new List<HealthCheckResult>();

        checks.Add(await CheckDatabaseAsync(cancellationToken));
        checks.Add(CheckDiscord());
        checks.Add(await CheckRedisAsync(cancellationToken));

        return BuildReport(checks);
    }

    private HealthStatusReport BuildReport(List<HealthCheckResult> checks) {
        var healthy = checks.All(c => c.Healthy);
        var status = healthy ? "Healthy" : "Unhealthy";
        return new HealthStatusReport(status, healthy, _timeProvider.GetUtcNow(), checks);
    }

    private async Task<HealthCheckResult> CheckDatabaseAsync(CancellationToken cancellationToken) {
        try {
            await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
            var canConnect = await db.Database.CanConnectAsync(cancellationToken);
            return new HealthCheckResult("database", canConnect, canConnect ? null : "Database not reachable.");
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Database readiness check failed");
            return new HealthCheckResult("database", false, ex.Message);
        }
    }

    private HealthCheckResult CheckDiscord() {
        if (_discordMultiplexer.IsReady)
            return new HealthCheckResult("discord", true, null);

        const string message = "Discord client not ready.";
        _logger.LogWarning("{Message}", message);
        return new HealthCheckResult("discord", false, message);
    }

    private async Task<HealthCheckResult> CheckRedisAsync(CancellationToken cancellationToken) {
        var connectionString = _configuration.GetConnectionString("Redis");
        if (string.IsNullOrWhiteSpace(connectionString)) {
            const string message = "Redis connection string missing.";
            _logger.LogWarning("{Message}", message);
            return new HealthCheckResult("redis", false, message);
        }

        try {
            var options = ConfigurationOptions.Parse(connectionString);
            options.AbortOnConnectFail = false;
            using var mux = await ConnectionMultiplexer.ConnectAsync(options);
            if (!mux.IsConnected)
                return new HealthCheckResult("redis", false, "Redis connection not established.");

            return new HealthCheckResult("redis", true, null);
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Redis readiness check failed");
            return new HealthCheckResult("redis", false, ex.Message);
        }
    }
}
