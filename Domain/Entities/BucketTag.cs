namespace Domain.Entities;

[Table("BucketTags")]
public class BucketTag {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    /// <summary>
    /// GUID of the bucket.
    /// </summary>
    public string BucketId { get; set; } = null!;

    /// <summary>
    /// Key.
    /// </summary>
    public string Key { get; set; } = null!;

    /// <summary>
    /// Value.
    /// </summary>
    public string Value { get; set; } = null!;

    /// <summary>
    /// Timestamp from record creation, in UTC time.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; set; }
}