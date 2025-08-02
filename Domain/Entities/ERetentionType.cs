namespace Domain.Entities;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ERetentionType {
    [EnumMember(Value = "NONE")]
    None,

    [EnumMember(Value = "GOVERNANCE")]
    Governance,

    [EnumMember(Value = "COMPLIANCE")]
    Compliance
}