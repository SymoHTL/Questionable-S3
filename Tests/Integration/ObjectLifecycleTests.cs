using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using Amazon.S3.Transfer;
using Tests.Infrastructure;

namespace Tests.Integration;

public class ObjectLifecycleTests : S3IntegrationTestBase {
    private const int LargeObjectSizeBytes = 500 * 1024 * 1024;

    [Test]
    public async Task PutAndGetObject_RoundTripsUtf8Payload() {
        var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"object-{Guid.NewGuid():N}.txt");
        const string payload = "Hello, Questionable S3!";

        await Client.PutObjectAsync(new PutObjectRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentType = "text/plain",
            ContentBody = payload
        });

        using var response = await Client.GetObjectAsync(bucket.Name, key);
        await using var responseStream = response.ResponseStream;
        using var reader = new StreamReader(responseStream, Encoding.UTF8);
        var content = await reader.ReadToEndAsync();

        Assert.That(response.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        Assert.That(content, Is.EqualTo(payload));
    }

    [Test]
    public async Task DeleteObject_RemovesStoredContent() {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"object-{Guid.NewGuid():N}.txt");
        const string payload = "Payload scheduled for deletion";

        await Client.PutObjectAsync(new PutObjectRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentType = "text/plain",
            ContentBody = payload
        });

        var deleteResponse = await Client.DeleteObjectAsync(bucket.Name, key);
        Assert.That(deleteResponse.HttpStatusCode,
            Is.EqualTo(System.Net.HttpStatusCode.NoContent).Or.EqualTo(System.Net.HttpStatusCode.OK));

        var ex = Assert.ThrowsAsync<Amazon.S3.AmazonS3Exception>(async () =>
            await Client.GetObjectAsync(bucket.Name, key));
        Assert.That(ex?.StatusCode, Is.EqualTo(System.Net.HttpStatusCode.NotFound));
    }

    [Test]
    public async Task PutAndGetLargeObject_PreservesContentWithTransferUtility() {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"large-object-{Guid.NewGuid():N}.bin");

        var buffer = new byte[LargeObjectSizeBytes];
        RandomNumberGenerator.Fill(buffer);
        var originalHash = MD5.HashData(buffer);

        await using (var uploadStream = new MemoryStream(buffer, writable: false)) {
            await TransferUtility.UploadAsync(new TransferUtilityUploadRequest() {
                BucketName = bucket.Name,
                Key = key,
                InputStream = uploadStream,
                ContentType = "application/octet-stream",
                AutoCloseStream = false
            });
        }

        using var getResponse = await Client.GetObjectAsync(bucket.Name, key);
        await using var downloadStream = new MemoryStream();
        await getResponse.ResponseStream.CopyToAsync(downloadStream);
        var downloadedBytes = downloadStream.ToArray();
        var downloadedHash = MD5.HashData(downloadedBytes);

        Assert.That(getResponse.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        Assert.That(downloadedBytes.Length, Is.EqualTo(LargeObjectSizeBytes));
        Assert.That(downloadedHash, Is.EqualTo(originalHash));
    }

    [Test]
    public async Task PutAndGetLargeObject_PreservesContent() {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"large-object-{Guid.NewGuid():N}.bin");

        var buffer = new byte[LargeObjectSizeBytes];
        RandomNumberGenerator.Fill(buffer);
        var originalHash = MD5.HashData(buffer);

        var init = await Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest() {
            BucketName = bucket.Name,
            Key = key,
            ContentType = "application/octet-stream"
        });

        if (string.IsNullOrEmpty(init.UploadId))
            Assert.Fail("Failed to initiate multipart upload, missing UploadId");
        
        var partSize = 10 * 1024 * 1024;
        var partETags = new List<PartETag>();
        try {
            for (var offset = 0; offset < LargeObjectSizeBytes; offset += partSize) {
                var size = Math.Min(partSize, LargeObjectSizeBytes - offset);
                await using var partStream = new MemoryStream(buffer, offset, size, writable: false);
                var uploadPartResponse = await Client.UploadPartAsync(new UploadPartRequest {
                    BucketName = bucket.Name,
                    Key = key,
                    UploadId = init.UploadId,
                    PartNumber = (offset / partSize) + 1,
                    InputStream = partStream,
                    IsLastPart = (offset + size) >= LargeObjectSizeBytes
                });
                partETags.Add(new PartETag(uploadPartResponse.PartNumber ?? 0, uploadPartResponse.ETag));
            }

            var complete = await Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest {
                BucketName = bucket.Name,
                Key = key,
                UploadId = init.UploadId,
                PartETags = partETags
            });

            Assert.That(complete.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        }
        catch (Exception) {
            await Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest {
                BucketName = bucket.Name,
                Key = key,
                UploadId = init.UploadId
            });
            throw;
        }

        using var getResponse = await Client.GetObjectAsync(bucket.Name, key);
        await using var downloadStream = new MemoryStream();
        await getResponse.ResponseStream.CopyToAsync(downloadStream);
        var downloadedBytes = downloadStream.ToArray();
        var downloadedHash = MD5.HashData(downloadedBytes);

        Assert.That(getResponse.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        Assert.That(downloadedBytes.Length, Is.EqualTo(LargeObjectSizeBytes));
        Assert.That(downloadedHash, Is.EqualTo(originalHash));
    }

    [Test]
    public async Task PutObject_WithSseS3_DecryptsOnRead() {
        const string payload = "Encrypted payload sample";

        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"sse-object-{Guid.NewGuid():N}.txt");

        var putResponse = await Client.PutObjectAsync(new PutObjectRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentBody = payload,
            ContentType = "text/plain",
            ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
        });

        Assert.That(putResponse.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        Assert.That(putResponse.ServerSideEncryptionMethod, Is.EqualTo(ServerSideEncryptionMethod.AES256));

        using var getResponse = await Client.GetObjectAsync(bucket.Name, key);
        using var reader = new StreamReader(getResponse.ResponseStream, Encoding.UTF8);
        var roundTripped = await reader.ReadToEndAsync();

        Assert.That(getResponse.ServerSideEncryptionMethod, Is.EqualTo(ServerSideEncryptionMethod.AES256));
        Assert.That(roundTripped, Is.EqualTo(payload));
    }

    [Test]
    public async Task GetObject_WithInvalidCredentials_IsDenied() {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"secured-object-{Guid.NewGuid():N}.txt");

        await Client.PutObjectAsync(new PutObjectRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentBody = "Access controlled content",
            ContentType = "text/plain"
        });

        using var rogueClient = new AmazonS3Client(
            new BasicAWSCredentials("invalid-ak", "invalid-sk"),
            Options.CreateClientConfig());

        var ex = Assert.ThrowsAsync<AmazonS3Exception>(() => rogueClient.GetObjectAsync(bucket.Name, key));
        Assert.That(ex?.StatusCode,
            Is.EqualTo(HttpStatusCode.Forbidden).Or.EqualTo(HttpStatusCode.Unauthorized));
        Assert.That(ex?.ErrorCode, Is.EqualTo("AccessDenied"));
    }

    [Test]
    public async Task MultipartUpload_CompletesAndIsReadable() {
        const int partSize = 12 * 1024 * 1024;
        const int partCount = 5;

        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"multipart-object-{Guid.NewGuid():N}.bin");
        var dataBuffer = new MemoryStream(partSize * partCount);
        var uploadId = (await Client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentType = "application/octet-stream"
        })).UploadId;

        var partETags = new List<PartETag>();
        try {
            for (var partNumber = 1; partNumber <= partCount; partNumber++) {
                var partData = new byte[partSize];
                RandomNumberGenerator.Fill(partData);
                await dataBuffer.WriteAsync(partData, 0, partSize);
                await using var partStream = new MemoryStream(partData, writable: false);

                var uploadPartResponse = await Client.UploadPartAsync(new UploadPartRequest {
                    BucketName = bucket.Name,
                    Key = key,
                    UploadId = uploadId,
                    PartNumber = partNumber,
                    InputStream = partStream,
                    IsLastPart = partNumber == partCount
                });

                partETags.Add(new PartETag(partNumber, uploadPartResponse.ETag));
            }

            var completeResponse = await Client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest {
                BucketName = bucket.Name,
                Key = key,
                UploadId = uploadId,
                PartETags = partETags
            });
            Assert.That(completeResponse.HttpStatusCode, Is.EqualTo(HttpStatusCode.OK));
        }
        catch (Exception) {
            await Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest {
                BucketName = bucket.Name,
                Key = key,
                UploadId = uploadId
            });
            throw;
        }

        var expectedBytes = dataBuffer.ToArray();
        using var completedObject = await Client.GetObjectAsync(bucket.Name, key);
        await using var downloaded = new MemoryStream();
        await completedObject.ResponseStream.CopyToAsync(downloaded);

        Assert.That(completedObject.HttpStatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(downloaded.ToArray(), Is.EqualTo(expectedBytes));
    }

    [Test]
    public async Task ObjectTags_PersistAcrossRequests() {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"tagged-object-{Guid.NewGuid():N}.txt");

        await Client.PutObjectAsync(new PutObjectRequest {
            BucketName = bucket.Name,
            Key = key,
            ContentBody = "Tagged content",
            ContentType = "text/plain"
        });

        var expectedTags = new List<Tag> {
            new() { Key = "env", Value = "integration" },
            new() { Key = "feature", Value = "tags" }
        };

        await Client.PutObjectTaggingAsync(new PutObjectTaggingRequest {
            BucketName = bucket.Name,
            Key = key,
            Tagging = new Tagging { TagSet = expectedTags }
        });

        var tagResponse = await Client.GetObjectTaggingAsync(new GetObjectTaggingRequest {
            BucketName = bucket.Name,
            Key = key
        });

        Assert.That(tagResponse.HttpStatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(tagResponse.Tagging.Count, Is.EqualTo(expectedTags.Count));
        foreach (var tag in expectedTags) {
            Assert.That(tagResponse.Tagging.Any(t => t.Key == tag.Key && t.Value == tag.Value), Is.True,
                $"Tag {tag.Key} missing or mismatched");
        }
    }
}