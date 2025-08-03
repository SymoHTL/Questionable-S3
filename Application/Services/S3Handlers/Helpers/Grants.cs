using System.Collections.Specialized;
using S3ServerLibrary.S3Objects;

namespace Application.Services.S3Handlers.Helpers;

public class Grants {
    public static async Task<List<Grant>> GrantsFromHeaders(IDbContext db, User user, NameValueCollection headers) {
        List<Grant> ret = [];
        if (headers is null || headers.Count < 1) return ret;

        string? headerVal;
        string[]? grantees;

        if (headers.AllKeys.Contains(Constants.Headers.AccessControlList.ToLower())) {
            headerVal = headers[Constants.Headers.AccessControlList.ToLower()];

            switch (headerVal) {
                case "private":
                    var grant = new Grant {
                        Permission = PermissionEnum.FullControl,
                        Grantee = new Grantee {
                            ID = user.Id,
                            DisplayName = user.Name
                        }
                    };
                    ret.Add(grant);
                    break;

                case "public-read":
                    grant = new Grant {
                        Permission = PermissionEnum.Read,
                        Grantee = new Grantee {
                            URI = "http://acs.amazonaws.com/groups/global/AllUsers",
                            DisplayName = Constants.UserGroups.AllUsers
                        }
                    };
                    ret.Add(grant);
                    break;

                case "public-read-write":
                    grant = new Grant {
                        Permission = PermissionEnum.Read,
                        Grantee = new Grantee {
                            URI = "http://acs.amazonaws.com/groups/global/AllUsers",
                            DisplayName = Constants.UserGroups.AllUsers
                        }
                    };
                    ret.Add(grant);

                    grant = new Grant {
                        Permission = PermissionEnum.Write,
                        Grantee = new Grantee {
                            URI = "http://acs.amazonaws.com/groups/global/AllUsers",
                            DisplayName = Constants.UserGroups.AllUsers
                        }
                    };
                    ret.Add(grant);
                    break;

                case "authenticated-read":
                    grant = new Grant {
                        Permission = PermissionEnum.Read,
                        Grantee = new Grantee {
                            URI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
                            DisplayName = Constants.UserGroups.AuthenticatedUsers
                        }
                    };
                    ret.Add(grant);
                    break;
            }
        }

        if (headers.AllKeys.Contains(Constants.Headers.AclGrantRead.ToLower())) {
            headerVal = headers[Constants.Headers.AclGrantRead.ToLower()];
            grantees = headerVal?.Split(',');
            if (grantees is { Length: > 0 }) {
                foreach (var curr in grantees) {
                    var (success, grant) = await GrantFromString(db, curr, PermissionEnum.Read);
                    if (!success) continue;
                    ret.Add(grant);
                }
            }
        }

        if (headers.AllKeys.Contains(Constants.Headers.AclGrantWrite.ToLower())) {
            headerVal = headers[Constants.Headers.AclGrantWrite.ToLower()];
            grantees = headerVal?.Split(',');
            if (grantees is { Length: > 0 }) {
                foreach (string curr in grantees) {
                    var (success, grant) = await GrantFromString(db, curr, PermissionEnum.Write);
                    if (!success) continue;
                    ret.Add(grant);
                }
            }
        }

        if (headers.AllKeys.Contains(Constants.Headers.AclGrantReadAcp.ToLower())) {
            headerVal = headers[Constants.Headers.AclGrantReadAcp.ToLower()];
            grantees = headerVal?.Split(',');
            if (grantees is { Length: > 0 }) {
                foreach (var curr in grantees) {
                    var (success, grant) = await GrantFromString(db, curr, PermissionEnum.ReadAcp);
                    if (!success) continue;
                    ret.Add(grant);
                }
            }
        }

        if (headers.AllKeys.Contains(Constants.Headers.AclGrantWriteAcp.ToLower())) {
            headerVal = headers[Constants.Headers.AclGrantWriteAcp.ToLower()];
            grantees = headerVal?.Split(',');
            if (grantees is { Length: > 0 }) {
                foreach (var curr in grantees) {
                    var (success, grant) = await GrantFromString(db, curr, PermissionEnum.WriteAcp);
                    if (!success) continue;
                    ret.Add(grant);
                }
            }
        }

        if (headers.AllKeys.Contains(Constants.Headers.AclGrantFullControl.ToLower())) {
            headerVal = headers[Constants.Headers.AclGrantFullControl.ToLower()];
            grantees = headerVal?.Split(',');
            if (grantees is { Length: > 0 }) {
                foreach (var curr in grantees) {
                    var (success, grant) = await GrantFromString(db, curr, PermissionEnum.FullControl);
                    if (!success) continue;
                    ret.Add(grant);
                }
            }
        }

        return ret;
    }


    public static async Task<(bool success, Grant grant)> GrantFromString(IDbContext db, string str, PermissionEnum permType) {
        Grant? grant = null;
        if (string.IsNullOrEmpty(str)) return (false, grant)!;

        var parts = str.Split('=');
        if (parts.Length != 2) return (false, grant)!;
        var granteeType = parts[0];
        var grantee = parts[1];

        grant = new Grant {
            Permission = permType,
            Grantee = new Grantee()
        };

        switch (granteeType) {
            case "emailAddress": {
                var user = await db.Users.ReadUserByEmailAsync(grantee);
                if (user is null)
                    return (false, grant);

                grant.Grantee.ID = user.Id;
                grant.Grantee.DisplayName = user.Name;
                return (true, grant);
            }
            case "id": {
                var user = await db.Users.ReadUserByIdAsync(grantee);
                if (user is null)
                    return (false, grant);
                grant.Grantee.ID = user.Id;
                grant.Grantee.DisplayName = user.Name;
                return (true, grant);
            }
            case "uri":
                grant.Grantee.URI = grantee;
                return (true, grant);
            default:
                return (false, grant);
        }
    }
}