namespace Domain.Entities;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EAuthenticationResult {
    /// <summary>
    ///     No authentication material was supplied.
    /// </summary>
    [EnumMember(Value = "NoMaterialSupplied")]
    NoMaterialSupplied,

    /// <summary>
    ///     The user was not found.
    /// </summary>
    [EnumMember(Value = "UserNotFound")]
    UserNotFound,

    /// <summary>
    ///     The supplied access key was not found.
    /// </summary>
    [EnumMember(Value = "AccessKeyNotFound")]
    AccessKeyNotFound,

    /// <summary>
    ///     Authentication was successful.
    /// </summary>
    [EnumMember(Value = "Authenticated")]
    Authenticated,

    /// <summary>
    ///     Authentication was not successful.
    /// </summary>
    [EnumMember(Value = "NotAuthenticated")]
    NotAuthenticated
}