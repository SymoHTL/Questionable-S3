namespace Domain.Entities;

[Table("ObjectTags")]
public class ObjectTag {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    public string BucketId { get; set; } = null!;

    [ForeignKey(nameof(Object))]
    public string ObjectId { get; set; } = null!;

    public string Key { get; set; } = null!;

    public string Value { get; set; } = null!;

    public DateTimeOffset CreatedUtc { get; set; }
    public Object Object { get; set; } = null!;
}