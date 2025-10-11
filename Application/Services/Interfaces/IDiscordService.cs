namespace Application.Services.Interfaces;

public interface IDiscordService {
    Task<ulong> CreateChannelAsync(string channelName);
    Task DeleteChannelAsync(ulong channelId);
}