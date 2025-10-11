using Discord;
using Discord.Rest;
using Discord.WebSocket;
using Infrastructure.Extensions;

namespace Infrastructure.Services;

public class DiscordMultiplexer : IDiscordService, IAsyncDisposable {
    private readonly ulong _guildId;
    private readonly string[] _tokens;
    private DiscordSocketClient[] _discordClients;
    private readonly ILogger<DiscordMultiplexer> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeProvider _timeProvider;

    public DiscordMultiplexer(IConfiguration configuration, ILogger<DiscordMultiplexer> logger, IServiceProvider serviceProvider, TimeProvider timeProvider) {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _timeProvider = timeProvider;
        _tokens = configuration.GetSection("Discord:Tokens").Get<string[]>()
                  ?? throw new InvalidOperationException("Discord tokens are not configured.");
        if (_tokens.Length == 0)
            throw new InvalidOperationException("No Discord tokens provided in configuration.");

        _guildId = configuration.GetSection("Discord:GuildId").Get<ulong>();
        if (_guildId == 0)
            throw new InvalidOperationException("Discord Guild ID is not configured or invalid.");

        _discordClients = new DiscordSocketClient[_tokens.Length];
        for (var i = 0; i < _tokens.Length; i++) {
            _discordClients[i] = new DiscordSocketClient(new DiscordSocketConfig() {
                LogLevel = LogSeverity.Info,
                DefaultRetryMode = RetryMode.RetryRatelimit,
                GatewayIntents = GatewayIntents.Guilds | GatewayIntents.MessageContent,
                AlwaysDownloadUsers = false,
                MessageCacheSize = 500,
            });
        }
    }

    public async Task StartAsync() {
        for (var i = 0; i < _tokens.Length; i++) {
            await _discordClients[i].LoginAsync(TokenType.Bot, _tokens[i]);
            await _discordClients[i].StartAsync();
            await WaitForClientReady(_discordClients[i]);
            HasGuild(_discordClients[i]);
            _logger.LogInformation("Discord client {Name} started", _discordClients[i].CurrentUser.Username);
            _discordClients[i].Log += LogMessage;
        }
    }

    private void HasGuild(DiscordSocketClient client) {
        if (client.Guilds.All(g => g.Id != _guildId))
            throw new InvalidOperationException(
                $"Guild with ID {_guildId} not found in client {client.CurrentUser.Username}");
    }

    private async Task WaitForClientReady(DiscordSocketClient client) {
        var tcs = new TaskCompletionSource();
        client.Ready += () => {
            tcs.SetResult();
            return Task.CompletedTask;
        };
        await tcs.Task;
    }

    private Task LogMessage(LogMessage logMessage) {
        if (_logger.IsEnabled(logMessage.Severity.ToLogLevel()))
            _logger.Log(logMessage.Severity.ToLogLevel(), logMessage.Exception,
                "Discord: {Source} - {Message}", logMessage.Source, logMessage.Message);
        return Task.CompletedTask;
    }

    public async Task<IMessageChannel> GetChannelAsync(ulong channelId) {
        var channel = await _discordClients[Random.Shared.Next(_discordClients.Length)]
            .GetChannelAsync(channelId);
        if (channel is not IMessageChannel msgChannel)
            throw new InvalidOperationException($"Channel with ID {channelId} is not a message channel.");
        return msgChannel;
    }

    public async Task<ulong> CreateChannelAsync(string channelName) {
        if (string.IsNullOrWhiteSpace(channelName))
            throw new ArgumentException("Channel name cannot be null or empty.", nameof(channelName));
        var client = _discordClients[Random.Shared.Next(_discordClients.Length)];
        var guild = client.Guilds.FirstOrDefault(g => g.Id == _guildId)
                    ?? client.GetGuild(_guildId) ??
                    throw new InvalidOperationException($"Guild with ID {_guildId} not found");

        var channel = await guild.CreateTextChannelAsync(channelName);
        return channel.Id;
    }

    public async Task DeleteChannelAsync(ulong channelId) {
        var client = _discordClients[Random.Shared.Next(_discordClients.Length)];

        try {
            var channel = await client.GetChannelAsync(channelId);

            switch (channel) {
                case SocketGuildChannel socketChannel:
                    await socketChannel.DeleteAsync();
                    return;
                case RestGuildChannel restChannel:
                    await restChannel.DeleteAsync();
                    return;
                case IGuildChannel guildChannelObj:
                    await guildChannelObj.DeleteAsync();
                    return;
            }

            var guild = client.Guilds.FirstOrDefault(g => g.Id == _guildId) ?? client.GetGuild(_guildId);
            if (guild is null) {
                _logger.LogWarning("Guild {GuildId} not found when deleting channel {ChannelId}", _guildId, channelId);
                return;
            }

            var guildChannel = guild.GetChannel(channelId);
            if (guildChannel is not null) {
                await guildChannel.DeleteAsync();
                return;
            }

            _logger.LogWarning("Channel {ChannelId} not found for deletion", channelId);
        }
        catch (Exception ex) {
            _logger.LogError(ex, "Failed to delete channel {ChannelId}", channelId);
            throw;
        }
    }

    public async Task RefreshObjectMessageAsync(ulong messageId, ulong channelId, CancellationToken ct) {
        var client = _discordClients[Random.Shared.Next(_discordClients.Length)];
        var channel = await client.GetChannelAsync(channelId) as IMessageChannel;
        if (channel is null) {
            _logger.LogWarning("Channel with ID {ChannelId} not found for message refresh",
                channelId);
            return;
        }

        var message = await channel.GetMessageAsync(messageId, options: new RequestOptions() { CancelToken = ct });
        if (message is null) {
            _logger.LogWarning("Message with ID {MessageId} not found in channel {ChannelId}",
                messageId, channelId);
            return;
        }
        
        using var scope = _serviceProvider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<IDbContext>();
        var objChunks = await db.ObjectChunks
            .AsTracking()
            .Where(c => c.MessageId == messageId)
            .ToListAsync(ct);
        if (objChunks.Count == 0) {
            _logger.LogWarning("No object chunks found for message {MessageId}", messageId);
            return;
        }

        foreach (var chunk in objChunks) {
            var attachment = message.Attachments.FirstOrDefault(a => a.Id == chunk.AttachmentId);
            if (attachment is null) {
                _logger.LogWarning("Attachment with ID {AttachmentId} not found in message {MessageId}",
                    chunk.AttachmentId, messageId);
                continue;
            }
            chunk.BlobUrl = attachment.Url;
            chunk.ExpireAt = _timeProvider.GetUtcNow() + ObjectChunk.ExpireAfter;
        }
        
        await db.SaveChangesAsync(ct);
    }

    public async Task BulkDeleteAsync(ulong[] ids, ulong channel, CancellationToken ct) {
        if (ids.Length == 0) return;

        var client = _discordClients[Random.Shared.Next(_discordClients.Length)];
        var messageChannel = await client.GetChannelAsync(channel) as IMessageChannel;
        if (messageChannel is null) {
            _logger.LogWarning("Channel with ID {ChannelId} not found for bulk delete", channel);
            return;
        }

        foreach (var id in ids) await messageChannel.DeleteMessageAsync(id, options: new RequestOptions { CancelToken = ct, RetryMode = RetryMode.AlwaysRetry});
    }

    public async ValueTask DisposeAsync() {
        foreach (var client in _discordClients) {
            if (client is not null) {
                try {
                    await client.LogoutAsync();
                    await client.DisposeAsync();
                }
                catch (Exception ex) {
                    // Log or handle the exception as needed
                    Console.WriteLine($"Error disposing Discord client: {ex.Message}");
                }
            }
        }

        _discordClients = null!;
    }
}