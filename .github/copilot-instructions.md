# Questionable-S3 Copilot Guide
## Architecture
- `Domain/` holds EF Core entities (e.g. `Domain/Entities/Object.cs`) and shared types like `RequestMetadata`; IDs default to `Ulid` and timestamps come from `TimeProvider`.
- `Application/` provides S3-facing logic: query helpers under `Application/QueryableExtensions/` and handlers in `Application/Services/S3Handlers/**` that translate `S3ServerLibrary` requests into DB operations via `IDbContext`.
- `Infrastructure/` wires external services: `QuestionableDbContext` (MySQL + DataProtection), Discord integration, Hangfire, and registers concrete handlers such as `DiscordBucketStore` in `Configuration/`.
- `WebApi/Program.cs` bootstraps a Cocona CLI, loads config, registers infrastructure, then runs `S3Manager` which wraps the embedded `S3Server`.
- `Tests/` contains AWS SDK integration tests hitting the local S3 endpoint; they assume the server is already running at `http://localhost:8080` with seed credentials.
## Request Flow
- Every request enters `S3SettingsHandler` (`Application/Services/S3Handlers/Settings`), which logs, shortcuts special routes, and calls `S3AuthHandler` to populate `RequestMetadata`.
- `S3AuthHandler` (`.../Auth/S3AuthHandler.cs`) authenticates via `Credentials` and layers bucket/object authorization using ACLs, public flags, and ownership.
- Object operations funnel through `S3ObjectHandler` (`.../Objects/S3ObjectHandler.cs`), which writes incoming payloads to `temp/` before delegating persistence to `IBucketStore` and applying ACL headers.
- Bucket creation in `S3BucketHandler` (`.../Buckets/S3BucketHandler.cs`) provisions a Discord channel via `IDiscordService` and persists ACL grants parsed by `Helpers/Grants.cs`.
## Discord Storage Model
- `DiscordBucketStore` (`Infrastructure/Services/DiscordBucketStore.cs`) chunks uploads into 10 MB slices, batches 10 attachments per Discord message, and records chunk metadata in `ObjectChunks`.
- Successful uploads enqueue Hangfire recurring jobs (`IRecurringJobManagerV2`) per Discord message to refresh CDN URLs via `DiscordMultiplexer.RefreshObjectMessageAsync` before the 23-hour expiry defined on `ObjectChunk.ExpireAfter`.
- Object deletions queue Hangfire background jobs to bulk-delete the corresponding Discord messages.
- `DiscordMultiplexer` manages multiple bot tokens, ensures guild presence, exposes `GetChannelAsync`/`CreateChannelAsync`, and refreshes chunk records using scoped `IDbContext` instances.
## Data & Persistence Conventions
- `Infrastructure/Configuration/DatabaseRegistrar.cs` configures MySQL 8 with `UseQuerySplittingBehavior.SplitQuery`, `NoTracking` by default, and seeds an admin user/credential from `appsettings.{Environment}.json` if the DB is empty.
- Always access EF via `IDbContext`; reuse async extension methods in `Application/QueryableExtensions` (e.g. `ReadObjectLatestMetadataAsync`) instead of ad-hoc LINQ.
- Use DI-provided `TimeProvider` (registered in `S3HandlerRegistrar`) for timestamps to keep tests deterministic and align with Hangfire refresh logic.
- `Constants.Headers` mirrors S3 header names; reuse them when reading/writing metadata-rich responses.
## Running & Testing
- Build with `dotnet build Questionable-S3.sln`; restore relies on the solution root.
- Run the service with `dotnet run --project WebApi` after ensuring MySQL (per `ConnectionStrings:DefaultConnection`), Redis (`ConnectionStrings:Redis`), and Discord credentials (`Discord:Tokens`, `GuildId`) are reachable.
- First run auto-creates schema via `UseDatabaseAsync` and persists DataProtection keys; delete responsibly when resetting state.
- Integration tests (`dotnet test Tests/Tests.csproj`) expect the S3 endpoint live, seeded credentials (`admin`/`admin`), and may create large temp files; clean `test-object-large.txt` afterwards.
- Object uploads rely on `temp/` in the working directory; maintain or clean that folder as part of maintenance tasks.
## Extension Points & Pitfalls
- When adding new background operations, prefer Hangfire scheduling (see `DiscordBucketStore` patterns) to keep jobs centralized.
- Keep public-facing responses aligned with the S3 spec provided by `S3ServerLibrary`; deviations in status codes or headers will break AWS SDK clients used in `Tests`.
- Respect the global alias `Object = Domain.Entities.Object` across projects to avoid namespace clashes with `System.Object`.
- Configuration secrets (Discord tokens, DB passwords) are currently in JSON; move them to environment-specific stores or user secrets before sharing code.
