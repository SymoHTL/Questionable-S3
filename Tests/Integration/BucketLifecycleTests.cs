using System.Threading.Tasks;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Tests.Infrastructure;

namespace Tests.Integration;

public class BucketLifecycleTests : S3IntegrationTestBase
{
    [Test]
    public async Task CreateBucket_ReportsExists()
    {
        var bucketName = Options.CreateUniqueBucketName();
        var createResponse = await Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName,
            UseClientRegion = true
        });

        Assert.That(createResponse.HttpStatusCode, Is.EqualTo(System.Net.HttpStatusCode.OK));

        await using var bucket = new BucketHandle(Client, bucketName);
        var exists = await AmazonS3Util.DoesS3BucketExistV2Async(Client, bucketName);
        Assert.That(exists, Is.True);
    }

    [Test]
    public async Task DeleteBucket_RemovesBucket()
    {
        var bucketName = Options.CreateUniqueBucketName();
        await Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName,
            UseClientRegion = true
        });

        await using var bucket = new BucketHandle(Client, bucketName);

        var deleteResponse = await Client.DeleteBucketAsync(bucketName);
        Assert.That(deleteResponse.HttpStatusCode,
            Is.EqualTo(System.Net.HttpStatusCode.NoContent).Or.EqualTo(System.Net.HttpStatusCode.OK));

        var exists = await AmazonS3Util.DoesS3BucketExistV2Async(Client, bucketName);
        Assert.That(exists, Is.False);
    }
}
