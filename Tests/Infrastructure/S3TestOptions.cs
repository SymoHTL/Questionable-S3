using Amazon;
using Amazon.Runtime;
using Amazon.S3;

namespace Tests.Infrastructure;

public sealed record S3TestOptions(
    string ServiceUrl,
    string RegionSystemName,
    string AccessKey,
    string SecretKey,
    string BucketPrefix)
{
    private static readonly RegionEndpoint DefaultRegion = RegionEndpoint.EUWest1;

    public static S3TestOptions Load()
    {
        static string Get(string key, string fallback) => Environment.GetEnvironmentVariable(key) ?? fallback;

        var serviceUrl = Get("S3_TEST_SERVICE_URL", "http://localhost:8080");
        var region = Get("S3_TEST_REGION", DefaultRegion.SystemName);
        var accessKey = Get("S3_TEST_ACCESS_KEY", "admin");
        var secretKey = Get("S3_TEST_SECRET_KEY", "admin");
        var bucketPrefix = Get("S3_TEST_BUCKET_PREFIX", "questionable-s3-tests-")
            .ToLowerInvariant();

        return new S3TestOptions(serviceUrl, region, accessKey, secretKey, bucketPrefix);
    }

    public BasicAWSCredentials CreateCredentials() => new(AccessKey, SecretKey);

    public AmazonS3Config CreateClientConfig()
    {
        var regionEndpoint = RegionEndpoint.GetBySystemName(RegionSystemName) ?? DefaultRegion;
        return new AmazonS3Config
        {
            RegionEndpoint = regionEndpoint,
            ServiceURL = ServiceUrl,
            ForcePathStyle = true,
            UseHttp = ServiceUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
        };
    }

    public string CreateUniqueBucketName() =>
        $"{BucketPrefix}{Guid.NewGuid():N}";
}
