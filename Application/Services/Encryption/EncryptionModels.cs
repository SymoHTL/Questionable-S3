namespace Application.Services.Encryption;

public record EncryptionRequest(string Algorithm, string? KeyId, string? Context);

public record EncryptionOutcome(
    string Algorithm,
    string? KeyId,
    string EncryptedFilePath,
    byte[] ProtectedDataKey,
    string MetadataJson,
    string PlaintextMd5Hex,
    long CiphertextLength);

public record EncryptionChunkMetadata(int Index, long PlaintextOffset, int PlaintextLength, long CiphertextOffset, int CiphertextLength, string Nonce, string Tag);

public record EncryptionMetadata(string Algorithm, string? KeyId, string? Context, int ChunkSize, IReadOnlyList<EncryptionChunkMetadata> Chunks);
