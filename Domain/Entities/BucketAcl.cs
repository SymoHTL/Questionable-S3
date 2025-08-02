namespace Domain.Entities;

[Table("BucketAcls")]
public class BucketAcl {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    /// <summary>
    ///     User group.
    /// </summary>
    public string UserGroup { get; set; } = null!;

    /// <summary>
    ///     Bucket GUID.
    /// </summary>
    public string BucketId { get; set; } = null!;

    /// <summary>
    ///     User GUID.
    /// </summary>
    public string UserId { get; set; } = null!;

    /// <summary>
    ///     GUID of the issuing user.
    /// </summary>
    public string IssuedByUserId { get; set; } = null!;

    /// <summary>
    ///     Permit read operations.
    /// </summary>
    public bool PermitRead { get; set; } = false;

    /// <summary>
    ///     Permit write operations.
    /// </summary>
    public bool PermitWrite { get; set; } = false;

    /// <summary>
    ///     Permit access control read operations.
    /// </summary>
    public bool PermitReadAcp { get; set; } = false;

    /// <summary>
    ///     Permit access control write operations.
    /// </summary>
    public bool PermitWriteAcp { get; set; } = false;

    /// <summary>
    ///     Permit full control.
    /// </summary>
    public bool FullControl { get; set; } = false;

    /// <summary>
    ///     Timestamp from record creation, in UTC time.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; set; }
}