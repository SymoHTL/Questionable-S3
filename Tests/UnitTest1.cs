using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace Tests;

public class Tests {

    private AmazonS3Client _client;
    
    [TearDown]
    public void TearDown() {
        _client?.Dispose();
    }

    [SetUp]
    public void Setup() {
        BasicAWSCredentials cred = new BasicAWSCredentials("admin", "admin");
        AmazonS3Config config = new AmazonS3Config {
            RegionEndpoint = RegionEndpoint.EUWest1,
            ServiceURL = "http://localhost:8080",
            ForcePathStyle = true,
            UseHttp = true,
        };
        _client = new AmazonS3Client(cred, config);
    }

    [Test]
    public async Task CreateBucket() {
        const string bucketName = "test-bucket";

        // Create a new bucket
        var createBucketRequest = new PutBucketRequest {
            BucketName = bucketName,
            UseClientRegion = true
        };

        var response = await _client.PutBucketAsync(createBucketRequest);
        
        Assert.That(response.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
    }
    
    [Test]
    public async Task PutObject() {
        const string bucketName = "test-bucket";
        const string key = "test-object.txt";
        const string content = "Hello, S3!";

        // Put an object into the bucket
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        var putRequest = new PutObjectRequest {
            BucketName = bucketName,
            Key = key,
            InputStream = stream,
            ContentType = "text/plain",
        };

        var response = await _client.PutObjectAsync(putRequest);
        
        Assert.That(response.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
    }

    private const ulong LargeFileSize = 5 * 1024 * 1024; // 100 MB
    [Test]
    public async Task PutLargeObject() {
        const string bucketName = "test-bucket";
        const string key = "test-object-large.txt";
        
        
        var fs = new FileStream("test-object-large.txt", FileMode.Create, FileAccess.ReadWrite);
        // Fill the file with dummy data
        var writer = new StreamWriter(fs);
        for (ulong i = 0; i < LargeFileSize; i++) {
            await writer.WriteAsync("A");
        }
        await writer.FlushAsync();
        fs.Seek(0, SeekOrigin.Begin);

        // Put an object into the bucket
        var putRequest = new PutObjectRequest {
            BucketName = bucketName,
            Key = key,
            InputStream = fs,
            ContentType = "text/plain",
        };

        var response = await _client.PutObjectAsync(putRequest);
        
        Assert.That(response.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
    }

    [Test]
    public async Task GetLargeObject() {
        const string bucketName = "test-bucket";
        const string key = "test-object-large.txt";

        // Get the object from the bucket
        var getRequest = new GetObjectRequest {
            BucketName = bucketName,
            Key = key
        };

        using var response = await _client.GetObjectAsync(getRequest);
        await using var responseStream = response.ResponseStream;
        
        Assert.That(response.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));
        
        using var reader = new StreamReader(responseStream);
        var content = await reader.ReadToEndAsync();
        
        Assert.That(content.Length, Is.EqualTo((int)LargeFileSize));
        Assert.That(content, Does.Contain("A"));
    }
}