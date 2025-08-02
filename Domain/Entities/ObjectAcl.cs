namespace Domain.Entities;

[Table("ObjectAcls")]
public class ObjectAcl {
    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    public string UserGroup { get; set; } = null!;

    public string UserId { get; set; } = null!;

    public string IssuedByUserId { get; set; } = null!;

    public string BucketId { get; set; } = null!;

    public string ObjectId { get; set; } = null!;

    public bool PermitRead { get; set; } = false;

    public bool PermitWrite { get; set; } = false;

    public bool PermitReadAcp { get; set; } = false;

    public bool PermitWriteAcp { get; set; } = false;

    public bool FullControl { get; set; } = false;

    public DateTimeOffset CreatedUtc { get; set; }
}