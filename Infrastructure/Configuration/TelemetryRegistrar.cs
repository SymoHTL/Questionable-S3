using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Exporter;
using OpenTelemetry.Instrumentation.EntityFrameworkCore;
using OpenTelemetry.Instrumentation.Hangfire;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Infrastructure.Configuration;

public static class TelemetryRegistrar {
    public static IServiceCollection AddTelemetry(this IServiceCollection services, IConfiguration configuration, ILoggingBuilder loggingBuilder) {
        var otlpSettings = configuration.GetSection("Otlp").Get<OtlpSettings>();
        ArgumentNullException.ThrowIfNull(otlpSettings, "Otlp settings are required");

        if (string.IsNullOrWhiteSpace(otlpSettings.Endpoint))
            throw new InvalidOperationException("Otlp endpoint configuration is required.");
        if (string.IsNullOrWhiteSpace(otlpSettings.ServiceName))
            throw new InvalidOperationException("Otlp service name configuration is required.");
        if (string.IsNullOrWhiteSpace(otlpSettings.ServiceVersion))
            throw new InvalidOperationException("Otlp service version configuration is required.");

        services.AddHealthChecks();

        var resourceBuilder = ResourceBuilder.CreateDefault()
            .AddService(serviceName: otlpSettings.ServiceName, serviceVersion: otlpSettings.ServiceVersion);

        services.AddOpenTelemetry()
            .WithMetrics(metrics => {
                metrics.SetResourceBuilder(resourceBuilder);

                metrics.AddAspNetCoreInstrumentation();
                metrics.AddHttpClientInstrumentation();
                metrics.AddRuntimeInstrumentation();
                metrics.AddProcessInstrumentation();

                metrics.AddMeter("System.Net.Http");
                metrics.AddView("http.server.request.duration",
                    new ExplicitBucketHistogramConfiguration {
                        Boundaries = [
                            0, 0.005, 0.01, 0.025, 0.05,
                            0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10
                        ]
                    });

                metrics.AddOtlpExporter(options => {
                    options.Endpoint = new Uri(otlpSettings.Endpoint);
                    options.Headers = otlpSettings.Header;
                });
            })
            .WithTracing(traces => {
                traces.SetResourceBuilder(resourceBuilder);

                traces.AddAspNetCoreInstrumentation();
                traces.AddHttpClientInstrumentation();
                traces.AddEntityFrameworkCoreInstrumentation();
                traces.AddHangfireInstrumentation();

                traces.AddOtlpExporter(options => {
                    options.Endpoint = new Uri(otlpSettings.Endpoint);
                    options.Headers = otlpSettings.Header;
                    options.Protocol = OtlpExportProtocol.Grpc;
                });
            });

        loggingBuilder.AddOpenTelemetry(options => {
            options.IncludeScopes = true;
            options.IncludeFormattedMessage = true;
            options.SetResourceBuilder(resourceBuilder);

            options.AddOtlpExporter(exporterOptions => {
                exporterOptions.Endpoint = new Uri(otlpSettings.Endpoint);
                exporterOptions.Headers = otlpSettings.Header;
                exporterOptions.Protocol = OtlpExportProtocol.Grpc;
            });
        });

        return services;
    }
}
