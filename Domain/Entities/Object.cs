namespace Domain.Entities;

[Table("DiscordObjects")]
public class Object {
    /// <summary>
    ///     Object expiration timestamp.
    /// </summary>
    public DateTimeOffset? ExpirationUtc = null;

    public List<ObjectChunk> FileChunks { get; set; } = [];

    [Key]
    public string Id { get; set; } = Ulid.NewUlid().ToString();

    /// <summary>
    ///     GUID of the bucket.
    /// </summary>
    public string BucketId { get; set; } = null!;

    /// <summary>
    ///     GUID of the owner.
    /// </summary>
    public string OwnerId { get; set; } = null!;

    /// <summary>
    ///     GUID of the author.
    /// </summary>
    public string AuthorId { get; set; } = null!;

    /// <summary>
    ///     Object key.
    /// </summary>
    public string Key { get; set; } = null!;

    /// <summary>
    ///     Content type.
    /// </summary>
    public string ContentType { get; set; } = "application/octet-stream";

    /// <summary>
    ///     Content length.
    /// </summary>
    public long ContentLength { get; set; } = 0;

    /// <summary>
    ///     Physical storage content length after transformations such as encryption.
    /// </summary>
    public long StorageContentLength { get; set; } = 0;

    /// <summary>
    ///     Object version.
    /// </summary>
    public long Version { get; set; } = 1;

    /// <summary>
    ///     ETag of the object.
    /// </summary>
    public string Etag { get; set; } = null!;

    /// <summary>
    ///     Retention type.
    /// </summary>
    public ERetentionType Retention { get; set; } = ERetentionType.None;

    /// <summary>
    ///     BLOB filename.
    /// </summary>
    public string BlobFilename { get; set; } = null!;

    /// <summary>
    ///     Indicates if the object is a folder, i.e. ends with '/' and has a content length of 0.
    /// </summary>
    public bool IsFolder { get; set; } = false;

    /// <summary>
    ///     Delete marker.
    /// </summary>
    public bool DeleteMarker { get; set; } = false;

    /// <summary>
    ///     MD5.
    /// </summary>
    public string Md5 { get; set; } = null!;

    /// <summary>
    ///     Indicates whether the object payload is encrypted at rest.
    /// </summary>
    public bool IsEncrypted { get; set; } = false;

    /// <summary>
    ///     Algorithm identifier (e.g. AES256, aws:kms).
    /// </summary>
    public string? EncryptionAlgorithm { get; set; }

    /// <summary>
    ///     Identifier of the master key used to protect the data key.
    /// </summary>
    public string? EncryptionKeyId { get; set; }

    /// <summary>
    ///     Serialized encryption metadata (chunk layout, nonces, tags, etc.).
    /// </summary>
    public string? EncryptionMetadata { get; set; }

    /// <summary>
    ///     Protected envelope data key (DPAPI/DataProtection payload).
    /// </summary>
    public byte[]? EncryptedDataKey { get; set; }

    /// <summary>
    ///     Optional encryption context provided by the client.
    /// </summary>
    public string? EncryptionContext { get; set; }

    /// <summary>
    ///     Creation timestamp.
    /// </summary>
    public DateTimeOffset CreatedUtc { get; set; }

    /// <summary>
    ///     Last update timestamp.
    /// </summary>
    public DateTimeOffset LastUpdateUtc { get; set; }

    /// <summary>
    ///     Last access timestamp.
    /// </summary>
    public DateTimeOffset LastAccessUtc { get; set; }
}