using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3.Model;
using NUnit.Framework;
using Tests.Infrastructure;

namespace Tests.Integration;

public class ObjectLifecycleTests : S3IntegrationTestBase
{
    private const int LargeObjectSizeBytes = 5 * 1024 * 1024; // 5 MB keeps runtime reasonable

    [Test]
    public async Task PutAndGetObject_RoundTripsUtf8Payload()
    {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"object-{Guid.NewGuid():N}.txt");
        const string payload = "Hello, Questionable S3!";

        await Client.PutObjectAsync(new PutObjectRequest
        {
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
    public async Task DeleteObject_RemovesStoredContent()
    {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"object-{Guid.NewGuid():N}.txt");
        const string payload = "Payload scheduled for deletion";

        await Client.PutObjectAsync(new PutObjectRequest
        {
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
    public async Task PutAndGetLargeObject_PreservesContent()
    {
        await using var bucket = await CreateEphemeralBucketAsync();
        var key = bucket.TrackKey($"large-object-{Guid.NewGuid():N}.bin");

        var buffer = new byte[LargeObjectSizeBytes];
        RandomNumberGenerator.Fill(buffer);
        var originalHash = MD5.HashData(buffer);

        await using (var uploadStream = new MemoryStream(buffer, writable: false))
        {
            await Client.PutObjectAsync(new PutObjectRequest
            {
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
}
