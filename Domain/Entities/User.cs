namespace Domain.Entities;

[Table("Users")]
public class User {
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    public string Name { get; set; } = null!;

    public string Email { get; set; } = null!;

    public DateTimeOffset CreatedUtc { get; set; }
}