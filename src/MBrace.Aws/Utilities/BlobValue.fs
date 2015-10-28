namespace MBrace.Aws.Runtime.Utilities

open System
open System.IO
open System.Runtime.Serialization

open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Aws
open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities

open Amazon.S3.Model

/// Represents a value that has been persisted to blob store
[<Sealed; DataContract>]
type BlobValue<'T> internal (account : AwsS3Account, bucketName : string, key : string) =    
    [<DataMember(Name = "account")>]
    let account = account

    [<DataMember(Name = "bucketName")>]
    let bucketName = bucketName

    [<DataMember(Name = "key")>]
    let key = key

    /// Bucket to blob
    member __.Bucket = bucket

    /// Key to blob persisting the value
    member __.Key = key

    /// Asynchronously gets the blob size in bytes
    member __.GetSize() : Async<int64> = async {
        let req  = GetObjectMetadataRequest(BucketName = bucketName, Key = key)
        let! ct = Async.CancellationToken
        let! res = account.S3Client.GetObjectMetadataAsync(req, ct)
                   |> Async.AwaitTaskCorrect

        return res.ContentLength
    }

    /// Asynchronously gets the persisted value
    member __.GetValue() : Async<'T> = async {
        let req  = GetObjectRequest(BucketName = bucketName, Key = key)
        let! ct = Async.CancellationToken
        let! res = account.S3Client.GetObjectAsync(req, ct)
                   |> Async.AwaitTaskCorrect

        return ProcessConfiguration.BinarySerializer.Deserialize<'T>(res.ResponseStream)
    }

    /// Try reading the persisted value, returning Some t if file exists, None if it doesn't exist.
    member this.TryGetValue() : Async<'T option> = async {
        // since there's no way to efficiently test if an object exists without doing an API call
        // it's cheaper to just use exception here
        let! res = Async.Catch <| this.GetValue()
        match res with
        | Choice1Of2 x -> return Some x
        | _ -> return None
    }

    /// Asynchronously checks if blob exists in store
    member this.Exists() = async {
        // again, since there's no efficient way to check for object existance against S3 API
        // it's cheaper to just use exception here
        let! res = Async.Catch <| this.GetSize()
        match res with
        | Choice1Of2 _ -> return true
        | _ -> return false
    }

    /// Asynchronously deletes the blob
    member __.Delete() = async {
        let req  = DeleteObjectRequest(BucketName = bucketName, Key = key)
        let! ct = Async.CancellationToken
        do! account.S3Client.DeleteObjectAsync(req, ct)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }        

    /// Asynchronously writes a value to the specified blob
    member __.WriteValue(value : 'T) = async {
        let req = PutObjectRequest(BucketName = bucketName, Key = key)
        req.Timeout <- Nullable<_>(TimeSpan.FromMinutes 40.)

        use stream = new MemoryStream()
        ProcessConfiguration.BinarySerializer.Serialize<'T>(stream, value)
        do! stream.FlushAsync()

        req.InputStream <- stream
        let! ct = Async.CancellationToken
        do! account.S3Client.PutObjectAsync(req, ct)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

/// Represents a value that has been persisted
type BlobValue =
    /// <summary>
    ///     Defines a blob value with given parameters.
    /// </summary>
    /// <param name="account">AWS S3 account.</param>
    /// <param name="container">Bucket to blob.</param>
    /// <param name="path">Blob key.</param>
    static member Define<'T>(account : AwsS3Account, bucketName : string, key : string) = 
        new BlobValue<'T>(account, bucketName, key)