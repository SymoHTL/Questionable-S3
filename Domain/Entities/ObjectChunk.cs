namespace Domain.Entities;

[Table("Chunks")]
public class ObjectChunk {
    [Key]
    public ulong AttachmentId { get; set; }

    public ulong MessageId { get; set; }

    public long StartByte { get; set; }
    public long EndByte { get; set; }
    public long Size { get; set; }

    public string BlobUrl { get; set; } = null!;

    [DataType(DataType.DateTime)]
    public DateTimeOffset ExpireAt { get; set; }

    public string ObjectId { get; set; } = null!;

    [ForeignKey(nameof(ObjectId))]
    public Object Object { get; set; } = null!;

    [NotMapped]
    public static TimeSpan ExpireAfter { get; } = TimeSpan.FromHours(23);
}