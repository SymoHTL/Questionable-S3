using Application.Services.Health;

namespace Application.Services.Interfaces;

public interface IHealthStatusService {
    Task<HealthStatusReport> GetLivenessAsync(CancellationToken cancellationToken = default);
    Task<HealthStatusReport> GetReadinessAsync(CancellationToken cancellationToken = default);
}
