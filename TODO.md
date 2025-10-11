# TODO

## Service-Level Features

- [ ] Implement `ListBuckets` so authenticated users can enumerate accessible buckets.
- [ ] Add health/ready endpoints or CLI commands to aid container orchestration health checks.

## Bucket Endpoints

- [ ] Support bucket read operations: `GetBucketLocation`, `GetBucketLogging`, `GetBucketTagging`, `GetBucketVersioning`, `GetBucketWebsite`, ACL and statistics queries.
- [ ] Implement bucket mutation endpoints for ACL updates, logging configuration, tag management, versioning toggles, lifecycle policies, and website hosting metadata.
- [ ] Provide object listing endpoints (`ListObjectsV1/V2`, `ListObjectVersions`) with pagination aligned with AWS SDK expectations.

## Object Endpoints

- [ ] Add HEAD support (`ObjectExists`, `HeadObject`) to expose metadata without streaming content.
- [ ] Implement ranged GETs (`GetObject` with byte ranges) and conditional requests (ETag / If-Modified-Since headers).
- [ ] Support bulk delete (`DeleteObjects`), object ACL read/write, tagging operations, legal hold, and retention APIs referenced in `S3RequestType`.
- [ ] Expose object metadata queries such as `GetObjectAttributes` and nuanced error codes for delete markers/versioned objects.

## Cross-Cutting

- [ ] Harden Discord storage error handling with retries/backoff and enrich logging for message refresh jobs.
- [ ] Capture structured metrics (Prometheus/OpenTelemetry) for request latency and background job outcomes.
- [ ] Expand integration test coverage to include ACL enforcement, multipart upload flows, and tag persistence once the endpoints exist.
