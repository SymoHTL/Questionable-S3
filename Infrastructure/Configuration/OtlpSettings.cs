namespace Infrastructure.Configuration;

public class OtlpSettings {
    public string Endpoint { get; set; } = null!;
    public string Header { get; set; } = string.Empty;
    public string ServiceName { get; set; } = null!;
    public string ServiceVersion { get; set; } = null!;
}
