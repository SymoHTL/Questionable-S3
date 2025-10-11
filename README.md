# Questionable-S3

Questionable-S3 is a custom S3-compatible server backed by MySQL, Redis, and Discord for object storage. The WebAPI hosts a Cocona-powered CLI that boots the embedded `S3ServerLibrary` implementation and serves requests on port 8080.

## Project Layout

- `Domain/` & `Application/`: Entities, repository helpers, and domain services that translate S3 requests into database operations.
- `Infrastructure/`: EF Core context, Discord integration, Hangfire scheduling, and storage drivers (`DiscordBucketStore`).
- `WebApi/`: Cocona entrypoint that wires configuration, runs migrations, and starts `S3Manager`.
- `Tests/`: NUnit integration tests that exercise the server via the AWS SDK once the endpoint is running.

## Prerequisites

- .NET 9 SDK
- MySQL 8.x accessible via `ConnectionStrings:DefaultConnection`
- Redis 7.x accessible via `ConnectionStrings:Redis`
- Discord bot credentials configured with access to the target guild

Environment variables override `appsettings.*.json`; use the double underscore (`__`) separator for nested keys (e.g. `Discord__Tokens__0`).

## Local Development

```pwsh
# Restore and build the solution
 dotnet build Questionable-S3.sln

# Run the S3 manager locally (requires MySQL/Redis/Discord services)
 dotnet run --project WebApi
```

Secrets such as Discord tokens should be provided via environment variables or user secrets rather than the JSON files.

## Docker

### Docker Compose

A ready-to-run compose file is provided to orchestrate the app, MySQL, and Redis.

```pwsh
# Optional: provide Discord credentials and guild id
 $env:DISCORD_TOKEN = "<bot token>"
 $env:DISCORD_GUILD_ID = "123456789012345678"

# Build and start the stack
 docker compose up --build
```

The S3 endpoint will be available at `http://localhost:8080`. The MySQL root password defaults to `example` and the database to `questionable-s3`.

### Single Container

To run just the WebApi container (with external services already available):

```pwsh
 docker build -t questionable-s3:latest -f WebApi/Dockerfile .
 docker run --rm \
   -p 8080:8080 \
   -e ConnectionStrings__DefaultConnection="server=<mysql-host>;port=3306;database=questionable-s3;user=root;password=<pw>" \
   -e ConnectionStrings__Redis="password=,<redis-host>:6379" \
   -e Discord__Tokens__0="<bot token>" \
   -e Discord__GuildId="<guild id>" \
   questionable-s3:latest
```

Ensure the external MySQL and Redis instances are reachable from the container network.

## Testing

The `Tests` project contains NUnit-based integration tests that use the AWS SDK. Start the S3 server locally (or via Docker) before running the suite:

```pwsh
 dotnet test Tests/Tests.csproj
```

Test endpoints and credentials can be customized via environment variables:

- `S3_TEST_SERVICE_URL` (default `http://localhost:8080`)
- `S3_TEST_REGION` (default `eu-west-1`)
- `S3_TEST_ACCESS_KEY` / `S3_TEST_SECRET_KEY` (default `admin` / `admin`)
- `S3_TEST_BUCKET_PREFIX` (default `questionable-s3-tests-`)

## Roadmap

See `TODO.md` for the backlog of missing S3 endpoints and infrastructure enhancements.
