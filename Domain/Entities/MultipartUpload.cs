namespace Domain.Entities;

[Table("MultipartUploads")]
public class MultipartUpload {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();
    
    [Required]
    public string BucketId { get; set; } = null!;

    [Required]
    public string Key { get; set; } = null!;

    [Required]
    public string OwnerId { get; set; } = null!;
[Required]
    public string OwnerDisplayName { get; set; } = null!;
[Required]
    public string InitiatorId { get; set; } = null!;
[Required]
    public string InitiatorDisplayName { get; set; } = null!;

    [Required] public string ContentType { get; set; } = "application/octet-stream";

    /// <summary>
    ///     Directory where the individual part payloads are persisted until completion.
    /// </summary>
    /// [Required]
    public string UploadDirectory { get; set; } = null!;

    public DateTimeOffset CreatedUtc { get; set; }

    public DateTimeOffset LastUpdatedUtc { get; set; }
    public bool IsAborted { get; set; } = false;

    public bool UseServerSideEncryption { get; set; } = false;
    public string? EncryptionAlgorithm { get; set; }
    public string? EncryptionKeyId { get; set; }
    public string? EncryptionContext { get; set; }

    public List<MultipartUploadPart> Parts { get; set; } = [];
}
