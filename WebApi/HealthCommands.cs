using System.Text.Json;
using Cocona;
using Application.Services.Health;
using Application.Services.Interfaces;

namespace WebApi;

public class HealthCommands {
    private readonly IHealthStatusService _healthStatusService;

    public HealthCommands(IHealthStatusService healthStatusService) {
        _healthStatusService = healthStatusService;
    }

    [Command("health")]
    public async Task<int> HealthAsync(CancellationToken cancellationToken = default) {
        var report = await _healthStatusService.GetLivenessAsync(cancellationToken);
        WriteReport(report);
        return report.Healthy ? 0 : 1;
    }

    [Command("ready")]
    public async Task<int> ReadyAsync(CancellationToken cancellationToken = default) {
        var report = await _healthStatusService.GetReadinessAsync(cancellationToken);
        WriteReport(report);
        return report.Healthy ? 0 : 1;
    }

    private static void WriteReport(HealthStatusReport report) {
        var payload = new {
            report.Status,
            report.Healthy,
            report.Timestamp,
            Checks = report.Checks.Select(c => new { c.Name, c.Healthy, c.Error }).ToArray()
        };

        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);
    }
}
