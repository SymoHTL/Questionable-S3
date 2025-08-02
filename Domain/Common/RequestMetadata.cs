namespace Domain.Common;

public class RequestMetadata {
    public User User = null!;

    public Credential Credential = null!;

    public Bucket Bucket = null!;

    public List<BucketAcl> BucketAcls = [];

    public List<BucketTag> BucketTags = [];

    public DcObject Obj = null!;

    public List<ObjectAcl> ObjectAcls = [];

    public List<ObjectTag> ObjectTags = [];

    public EAuthenticationResult Authentication = EAuthenticationResult.NotAuthenticated;

    public EAuthorizationResult Authorization = EAuthorizationResult.NotAuthorized;
}