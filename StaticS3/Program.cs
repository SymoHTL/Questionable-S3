using S3ServerLibrary;
using S3ServerLibrary.S3Objects;
using WatsonWebserver.Core;

S3ServerSettings s3Settings = new S3ServerSettings() {
    Webserver = new WebserverSettings() {
        Hostname = "localhost",
        Port = 8080
    }
};

using var server = new S3Server(s3Settings);

server.Settings.DefaultRequestHandler = ctx => {
    Console.WriteLine($"No handler for request: {ctx.Http.Request.Url.Full} - {ctx.Http.Request.Method} - {ctx.Request.RequestType}");
    ctx.Response.StatusCode = 501; // Not Implemented
    return Task.CompletedTask;
};

server.Service.ListBuckets = ctx => {
    Console.WriteLine("ListBuckets called");
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new ListAllMyBucketsResult());
};

server.Bucket.Write = ctx => {
    Console.WriteLine("Bucket.Write called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Bucket.Delete = ctx => {
    Console.WriteLine("Bucket.Delete called");
    ctx.Response.StatusCode = 204; // No Content
    return Task.CompletedTask;
};

server.Bucket.ReadVersions = ctx => {
    Console.WriteLine("Bucket.ReadVersions called");
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new ListVersionsResult());
};

server.Object.CreateMultipartUpload = ctx => {
    Console.WriteLine("Object.CreateMultipartUpload called");
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new InitiateMultipartUploadResult {
        Bucket = ctx.Request.Bucket,
        Key = ctx.Request.Key,
        UploadId = Guid.NewGuid().ToString()
    });
};

server.Object.Delete = ctx => {
    Console.WriteLine("Object.Delete called");
    ctx.Response.StatusCode = 204; // No Content
    return Task.CompletedTask;
};


server.Object.UploadPart = ctx => {
    Console.WriteLine("Object.UploadPart called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.CompleteMultipartUpload = (ctx, multipartContext) => {
    Console.WriteLine("Object.CompleteMultipartUpload called with parts:");
    foreach (var part in multipartContext.Parts) {
        Console.WriteLine($" - PartNumber: {part.PartNumber}, ETag: {part.ETag}");
    }
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new CompleteMultipartUploadResult {
        Bucket = ctx.Request.Bucket,
        Key = ctx.Request.Key,
        Location = $"http://{ctx.Http.Request.Url.Host}/{ctx.Request.Bucket}/{ctx.Request.Key}",
        ETag = "\"dummy-etag\""
    });
};

server.Object.AbortMultipartUpload = ctx => {
    Console.WriteLine("Object.AbortMultipartUpload called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};


server.Object.Read = ctx => {
    Console.WriteLine("Object.Read called");
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new S3Object() {
        Key = ctx.Request.Key,
        ContentType = "text/plain",
        Data = new MemoryStream("Hello, World!"u8.ToArray())
    });
};

server.Object.Write = ctx => {
    Console.WriteLine("Object.Write called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.WriteAcl = (ctx, acsPolicy) => {
    Console.WriteLine("Object.WriteAcl called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.WriteTagging = (ctx, tags) => {
    Console.WriteLine("Object.WriteTagging called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.WriteRetention = (ctx, retention) => {
    Console.WriteLine("Object.WriteRetention called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.WriteLegalHold = (ctx, legalHold) => {
    Console.WriteLine("Object.WriteLegalHold called");
    ctx.Response.StatusCode = 200;
    return Task.CompletedTask;
};

server.Object.ReadAcl = ctx => {
    Console.WriteLine("Object.ReadAcl called");
    ctx.Response.StatusCode = 200;
    return Task.FromResult(new AccessControlPolicy());
};

    
    
server.Start();

Console.WriteLine("S3 Server started. Press Enter to stop.");

Console.ReadLine();
