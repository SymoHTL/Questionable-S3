namespace Domain.Entities;

[Table("Credentials")]
public class Credential {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    /// <summary>
    ///     User GUID.
    /// </summary>
    public string UserId { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    ///     Description.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    ///     Access key.
    /// </summary>
    public string AccessKey { get; set; } = null!;

    /// <summary>
    ///     Secret key.
    /// </summary>
    public string SecretKey { get; set; } = null!;

    /// <summary>
    ///     Indicates if the secret key is base64 encoded.
    /// </summary>
    public bool IsBase64 { get; set; } = false;

    /// <summary>
    ///     Timestamp from record creation, in UTC time.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; set; }
}