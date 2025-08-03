using Discord;

namespace Infrastructure.Extensions;

public static class LogSeverityExtensions {
    public static LogLevel ToLogLevel(this LogSeverity severity) {
        return severity switch {
            LogSeverity.Debug => LogLevel.Debug,
            LogSeverity.Info => LogLevel.Information,
            LogSeverity.Warning => LogLevel.Warning,
            LogSeverity.Error => LogLevel.Error,
            LogSeverity.Critical => LogLevel.Critical,
            _ => LogLevel.None
        };
    }
}