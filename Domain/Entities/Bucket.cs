namespace Domain.Entities;

[Table("Buckets")]
public class Bucket {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    public string OwnerGuid { get; set; } = Ulid.NewUlid().ToString();

    public string Name { get; set; } = null!;

    public string RegionString { get; set; } = "eu-west-1";

    public EStorageDriverType StorageType { get; set; } = EStorageDriverType.Discord;

    public bool EnableVersioning { get; set; } = false;

    public bool EnablePublicWrite { get; set; } = false;

    public bool EnablePublicRead { get; set; } = false;

    public DateTime CreatedUtc { get; set; }
}