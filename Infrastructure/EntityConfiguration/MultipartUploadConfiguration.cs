using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.EntityConfiguration;

public class MultipartUploadConfiguration : IEntityTypeConfiguration<MultipartUpload> {
    public void Configure(EntityTypeBuilder<MultipartUpload> builder) {
        builder.Navigation(u => u.Parts).AutoInclude();
    }
}
