using System.Buffers;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using Application.Services.Encryption;
using Microsoft.AspNetCore.DataProtection;

namespace Infrastructure.Services;

public class ObjectEncryptionService : IObjectEncryptionService {
    private const int DefaultChunkSize = 10 * 1024 * 1024; // 10 MB, aligned with Discord chunking
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IDataProtector _protector;

    public ObjectEncryptionService(IDataProtectionProvider dataProtectionProvider) {
        ArgumentNullException.ThrowIfNull(dataProtectionProvider);
        _protector = dataProtectionProvider.CreateProtector("QuestionableS3.ObjectEncryption");
    }

    public async Task<EncryptionOutcome> EncryptAsync(string sourceFilePath, EncryptionRequest request, CancellationToken cancellationToken = default) {
        ArgumentException.ThrowIfNullOrEmpty(sourceFilePath);
        ArgumentNullException.ThrowIfNull(request);
        if (!File.Exists(sourceFilePath)) throw new FileNotFoundException("Source file for encryption not found.", sourceFilePath);

        var tempEncryptedPath = Path.Combine(Path.GetDirectoryName(sourceFilePath) ?? Directory.GetCurrentDirectory(),
            $"{Path.GetFileName(sourceFilePath)}.{Ulid.NewUlid()}.enc");

        var dataKey = RandomNumberGenerator.GetBytes(32);
        var protectedDataKey = _protector.Protect(dataKey);

        var metadataChunks = new List<EncryptionChunkMetadata>();
        long plaintextOffset = 0;
        long ciphertextOffset = 0;
        var chunkIndex = 0;

        using var plaintextStream = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var ciphertextStream = new FileStream(tempEncryptedPath, FileMode.Create, FileAccess.Write, FileShare.None);

        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);

        var buffer = ArrayPool<byte>.Shared.Rent(DefaultChunkSize);
        try {
            int bytesRead;
            while ((bytesRead = await plaintextStream.ReadAsync(buffer.AsMemory(0, DefaultChunkSize), cancellationToken)) > 0) {
                md5.AppendData(buffer, 0, bytesRead);

                var nonce = RandomNumberGenerator.GetBytes(12);
                var tag = new byte[16];
                var ciphertextChunk = new byte[bytesRead];

                using (var cipher = new AesGcm(dataKey, 16)) {
                    cipher.Encrypt(nonce, buffer.AsSpan(0, bytesRead), ciphertextChunk.AsSpan(0, bytesRead), tag);
                }

                await ciphertextStream.WriteAsync(ciphertextChunk.AsMemory(0, ciphertextChunk.Length), cancellationToken);

                metadataChunks.Add(new EncryptionChunkMetadata(
                    chunkIndex,
                    plaintextOffset,
                    bytesRead,
                    ciphertextOffset,
                    ciphertextChunk.Length,
                    Convert.ToBase64String(nonce),
                    Convert.ToBase64String(tag)));

                plaintextOffset += bytesRead;
                ciphertextOffset += ciphertextChunk.Length;
                chunkIndex++;
            }
        }
        finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        await ciphertextStream.FlushAsync(cancellationToken);

        var md5Hex = Convert.ToHexString(md5.GetHashAndReset());
        var metadata = new EncryptionMetadata(request.Algorithm, request.KeyId, request.Context, DefaultChunkSize, metadataChunks);
        var metadataJson = JsonSerializer.Serialize(metadata, JsonOptions);

        return new EncryptionOutcome(request.Algorithm, request.KeyId, tempEncryptedPath, protectedDataKey, metadataJson, md5Hex,
            ciphertextOffset);
    }

    public async Task<string> ComputeMd5HexAsync(string sourceFilePath, CancellationToken cancellationToken = default) {
        ArgumentException.ThrowIfNullOrEmpty(sourceFilePath);
        if (!File.Exists(sourceFilePath)) throw new FileNotFoundException("Source file for hashing not found.", sourceFilePath);

        await using var stream = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var hash = await MD5.HashDataAsync(stream, cancellationToken);
        return Convert.ToHexString(hash);
    }

    public async Task<Stream> DecryptAsync(Stream encryptedStream, Domain.Entities.Object obj, CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(encryptedStream);
        ArgumentNullException.ThrowIfNull(obj);

        if (!obj.IsEncrypted) return encryptedStream;
        if (string.IsNullOrWhiteSpace(obj.EncryptionMetadata) || obj.EncryptedDataKey is null)
            throw new InvalidOperationException($"Object {obj.Id} is marked encrypted but lacks metadata or data key.");

        var metadata = JsonSerializer.Deserialize<EncryptionMetadata>(obj.EncryptionMetadata, JsonOptions)
                       ?? throw new InvalidOperationException("Failed to parse encryption metadata.");

        if (!encryptedStream.CanSeek) {
            var buffer = new MemoryStream();
            await encryptedStream.CopyToAsync(buffer, cancellationToken);
            buffer.Position = 0;
            encryptedStream = buffer;
        }

        encryptedStream.Position = 0;
        var plaintextStream = new MemoryStream(capacity: (int)Math.Min(obj.ContentLength, int.MaxValue));

        var dataKey = _protector.Unprotect(obj.EncryptedDataKey);

        foreach (var chunk in metadata.Chunks.OrderBy(c => c.Index)) {
            encryptedStream.Position = chunk.CiphertextOffset;
            var ciphertext = new byte[chunk.CiphertextLength];
            await ReadExactAsync(encryptedStream, ciphertext, cancellationToken);

            var nonce = Convert.FromBase64String(chunk.Nonce);
            var tag = Convert.FromBase64String(chunk.Tag);
            var plaintextChunk = new byte[chunk.PlaintextLength];

            using (var cipher = new AesGcm(dataKey, 16)) {
                cipher.Decrypt(nonce, ciphertext.AsSpan(0, chunk.CiphertextLength), tag, plaintextChunk.AsSpan(0, chunk.PlaintextLength));
            }

            await plaintextStream.WriteAsync(plaintextChunk.AsMemory(0, chunk.PlaintextLength), cancellationToken);
        }

        plaintextStream.Position = 0;
        return plaintextStream;
    }

    private static async Task ReadExactAsync(Stream source, byte[] buffer, CancellationToken cancellationToken) {
        var totalRead = 0;
        while (totalRead < buffer.Length) {
            var read = await source.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken);
            if (read == 0) throw new EndOfStreamException("Unexpected end of stream while reading encrypted payload.");
            totalRead += read;
        }
    }
}
