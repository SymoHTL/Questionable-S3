using Application.Services.Encryption;

namespace Application.Services.Interfaces;

public interface IObjectEncryptionService {
    Task<EncryptionOutcome> EncryptAsync(string sourceFilePath, EncryptionRequest request, CancellationToken cancellationToken = default);
    Task<string> ComputeMd5HexAsync(string sourceFilePath, CancellationToken cancellationToken = default);
    Task<Stream> DecryptAsync(Stream encryptedStream, Domain.Entities.Object obj, CancellationToken cancellationToken = default);
}
