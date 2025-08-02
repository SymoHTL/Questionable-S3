namespace Domain.Entities;

[Table("BucketStatistics")]
public class BucketStatistics {
    /// <summary>
    ///     The number of bytes for all objects in the bucket.
    /// </summary>
    public long Bytes = 0;

    /// <summary>
    ///     The number of objects in the bucket including all versions.
    /// </summary>
    public long Objects = 0;

    public string Name { get; set; } = null!;

    /// <summary>
    ///     GUID of the bucket.
    /// </summary>
    public string Id { get; set; } = Guid.NewGuid().ToString();
}