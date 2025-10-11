namespace Domain.Entities;

[Table("MultipartUploadParts")]
public class MultipartUploadPart {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    public string UploadId { get; set; } = null!;

    [ForeignKey(nameof(UploadId))]
    public MultipartUpload Upload { get; set; } = null!;

    public int PartNumber { get; set; }

    public long Size { get; set; }
    public string Etag { get; set; } = null!;

    public string TempFilePath { get; set; } = null!;

    public DateTimeOffset CreatedUtc { get; set; }

    public DateTimeOffset LastUpdatedUtc { get; set; }
}
