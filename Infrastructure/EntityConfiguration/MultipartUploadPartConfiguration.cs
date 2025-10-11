using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.EntityConfiguration;

public class MultipartUploadPartConfiguration : IEntityTypeConfiguration<MultipartUploadPart> {
    public void Configure(EntityTypeBuilder<MultipartUploadPart> builder) {
        builder.HasIndex(p => new { p.UploadId, p.PartNumber })
            .IsUnique();

        builder.Property(p => p.Etag)
            .IsRequired();

        builder.Property(p => p.TempFilePath)
            .IsRequired();

        builder.HasOne(p => p.Upload)
            .WithMany(u => u.Parts)
            .HasForeignKey(p => p.UploadId)
            .OnDelete(DeleteBehavior.Cascade);
    }
}