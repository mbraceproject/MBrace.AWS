#I "../../bin"
#r "FsPickler.dll"
#r "AWSSDK.Core.dll"
#r "AWSSDK.S3.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "AWSSDK.SQS.dll"
#r "Vagabond.dll"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.AWS.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2

open MBrace.Core
open MBrace.Core.Internals
open MBrace.AWS.Runtime
open MBrace.AWS.Store

let account = AwsAccount.Create("Default", RegionEndpoint.EUCentral1)
let store = S3FileStore.Create(account) :> ICloudFileStore

let run x = Async.RunSynchronously x

let clearBuckets() = async {
    let! buckets = store.EnumerateDirectories "/"
    let! _ = buckets |> Seq.filter (fun b -> b.StartsWith "/mbrace") |> Seq.map (fun b -> store.DeleteDirectory(b,true)) |> Async.Parallel
    return ()
}

clearBuckets() |> run

/////////////////////

open Amazon.S3
open Amazon.S3.Model
open System
open System.Net

let s3Client = account.S3Client

let rec ensureBucketCreated (bucketName : string) =
    let r = s3Client.ListBuckets()
    if r.Buckets |> Seq.exists(fun b -> b.BucketName = bucketName) |> not then
        let success =
            try let _ = s3Client.PutBucket(bucketName) in true
            with :? AmazonS3Exception as e when e.StatusCode = HttpStatusCode.Conflict -> false

        if not success then ensureBucketCreated bucketName


let test () =
    let bucketName = "mbrace" + Guid.NewGuid().ToString("N")
    [|1 .. 100|] |> Array.Parallel.iter (fun _ -> ensureBucketCreated bucketName)

    let resp = s3Client.InitiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName = bucketName, Key = "test"))
    let resp2 = s3Client.UploadPart(new UploadPartRequest(BucketName = resp.BucketName, Key = resp.Key, PartNumber = 1, UploadId = resp.UploadId, InputStream = new System.IO.MemoryStream([|1uy .. 255uy|])))
    let resp3 = s3Client.CompleteMultipartUpload(new CompleteMultipartUploadRequest(BucketName = resp.BucketName, Key = resp.Key, UploadId = resp.UploadId, PartETags = new ResizeArray<_>([new PartETag(1, resp2.ETag)])))
    ()

test ()