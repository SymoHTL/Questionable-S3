namespace Domain.Entities;

[Table("Chunks")]
public class DcFileChunk {
    [Key]
    public ulong AttachmentId { get; set; }

    public ulong MessageId { get; set; }

    public long StartByte { get; set; }
    public long EndByte { get; set; }
    public long Size { get; set; }

    public string BlobUrl { get; set; } = null!;

    [DataType(DataType.DateTime)]
    public DateTimeOffset ExpireAt { get; set; }

    public string FileId { get; set; } = null!;

    [ForeignKey(nameof(FileId))]
    public DcObject DcObject { get; set; } = null!;

    [NotMapped]
    public static TimeSpan ExpireAfter { get; } = TimeSpan.FromHours(23).Add(TimeSpan.FromMinutes(55));
}