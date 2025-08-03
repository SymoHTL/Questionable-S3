namespace Domain.Common;

public class Constants {
    public static class Headers {
        public const string RequestType = "X-Request-Type";
        public const string AuthenticationResult = "X-Authentication-Result";
        public const string AuthorizedBy = "X-Authorized-By";

        public const string DeleteMarker = "X-Amz-Delete-Marker";
        public const string AccessControlList = "X-Amz-Acl";
        public const string AclGrantRead = "X-Amz-Grant-Read";
        public const string AclGrantWrite = "X-Amz-Grant-Write";
        public const string AclGrantReadAcp = "X-Amz-Grant-Read-Acp";
        public const string AclGrantWriteAcp = "X-Amz-Grant-Write-Acp";
        public const string AclGrantFullControl = "X-Amz-Grant-Full-Control";
    }

    public static class UserGroups {
        public const string AllUsers = "AllUsers";
        public const string AuthenticatedUsers = "AuthenticatedUsers";
    }
    
    public static class File {
        public const string TempDir = "temp";
    }
    
    public static class HttpClients {
        public const string Discord = "Discord";
    }

    public class RecurringJobs {
        public static string FormatObjectRefresh(ulong messageId) {
            return $"RefreshDiscordMessage-{messageId}";
        }
    }
}