namespace Domain.Common;

public class Constants {
    public static class Headers {
        public static string RequestType = "X-Request-Type";
        public static string AuthenticationResult = "X-Authentication-Result";
        public static string AuthorizedBy = "X-Authorized-By";

        public static string DeleteMarker = "X-Amz-Delete-Marker";
        public static string AccessControlList = "X-Amz-Acl";
        public static string AclGrantRead = "X-Amz-Grant-Read";
        public static string AclGrantWrite = "X-Amz-Grant-Write";
        public static string AclGrantReadAcp = "X-Amz-Grant-Read-Acp";
        public static string AclGrantWriteAcp = "X-Amz-Grant-Write-Acp";
        public static string AclGrantFullControl = "X-Amz-Grant-Full-Control";
    }
}