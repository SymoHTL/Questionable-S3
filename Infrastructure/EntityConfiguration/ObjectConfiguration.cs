using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.EntityConfiguration;

public class ObjectConfiguration : IEntityTypeConfiguration<Object> {
    public void Configure(EntityTypeBuilder<Object> builder) {
        builder.Navigation(o => o.FileChunks).AutoInclude();
    }
}