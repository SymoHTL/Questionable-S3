using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using NUnit.Framework;

namespace Tests.Infrastructure;

public abstract class S3IntegrationTestBase
{
    private static bool? _endpointAvailable;

    protected static readonly S3TestOptions Options = S3TestOptions.Load();

    protected AmazonS3Client Client { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task GlobalSetUpAsync()
    {
        Client = new AmazonS3Client(Options.CreateCredentials(), Options.CreateClientConfig());

        if (_endpointAvailable is null)
        {
            _endpointAvailable = await EnsureEndpointAsync(Client);
        }

        if (_endpointAvailable is false)
        {
            Assert.Inconclusive("S3 endpoint is unavailable. Check the output for details.");
        }
    }

    [OneTimeTearDown]
    public void GlobalTearDown()
    {
        Client.Dispose();
    }

    protected async Task<BucketHandle> CreateEphemeralBucketAsync()
    {
        var bucketName = Options.CreateUniqueBucketName();
        await Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = bucketName,
            UseClientRegion = true
        });

        return new BucketHandle(Client, bucketName);
    }

    private static async Task<bool> EnsureEndpointAsync(IAmazonS3 client)
    {
        try
        {
            await client.ListBucketsAsync();
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode is HttpStatusCode.Forbidden or HttpStatusCode.NotImplemented)
        {
            TestContext.Progress.WriteLine($"ListBuckets returned {ex.StatusCode}. Continuing with tests.");
            return true;
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"Skipping S3 integration tests: {ex.Message}");
            return false;
        }
    }

    protected sealed class BucketHandle : IAsyncDisposable
    {
        private readonly IAmazonS3 _client;
        private readonly HashSet<string> _trackedKeys = new(StringComparer.Ordinal);

        public BucketHandle(IAmazonS3 client, string name)
        {
            _client = client;
            Name = name;
        }

        public string Name { get; }

        public string TrackKey(string key)
        {
            _trackedKeys.Add(key);
            return key;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var key in _trackedKeys)
            {
                try
                {
                    await _client.DeleteObjectAsync(new DeleteObjectRequest
                    {
                        BucketName = Name,
                        Key = key
                    });
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.NoContent)
                {
                    // Object already removed.
                }
            }

            try
            {
                await AmazonS3Util.DeleteS3BucketWithObjectsAsync(_client, Name);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.NoContent)
            {
                // Bucket already removed or never created.
            }
        }
    }
}
