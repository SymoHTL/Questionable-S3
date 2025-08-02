namespace Domain.Common;

public class RequestMetadata {
    public EAuthenticationResult Authentication = EAuthenticationResult.NotAuthenticated;

    public EAuthorizationResult Authorization = EAuthorizationResult.NotAuthorized;

    public Bucket Bucket = null!;

    public List<BucketAcl> BucketAcls = [];

    public List<BucketTag> BucketTags = [];

    public Credential Credential = null!;

    public DcObject Obj = null!;

    public List<ObjectAcl> ObjectAcls = [];

    public List<ObjectTag> ObjectTags = [];
    public User User = null!;
}