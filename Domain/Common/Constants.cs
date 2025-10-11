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
        public const string ServerSideEncryption = "x-amz-server-side-encryption";
        public const string ServerSideEncryptionAwsKmsKeyId = "x-amz-server-side-encryption-aws-kms-key-id";
        public const string ServerSideEncryptionContext = "x-amz-server-side-encryption-context";
        public const string ServerSideEncryptionCustomerAlgorithm = "x-amz-server-side-encryption-customer-algorithm";
        public const string ServerSideEncryptionCustomerKey = "x-amz-server-side-encryption-customer-key";
        public const string ServerSideEncryptionCustomerKeyMd5 = "x-amz-server-side-encryption-customer-key-MD5";
    }

    public static class UserGroups {
        public const string AllUsers = "AllUsers";
        public const string AuthenticatedUsers = "AuthenticatedUsers";
    }

    public static class EncryptionAlgorithms {
        public const string Aes256 = "AES256";
        public const string AwsKms = "aws:kms";
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