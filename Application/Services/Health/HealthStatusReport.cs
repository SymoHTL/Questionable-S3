namespace Application.Services.Health;

public record HealthStatusReport(
    string Status,
    bool Healthy,
    DateTimeOffset Timestamp,
    IReadOnlyList<HealthCheckResult> Checks);

public record HealthCheckResult(string Name, bool Healthy, string? Error);
