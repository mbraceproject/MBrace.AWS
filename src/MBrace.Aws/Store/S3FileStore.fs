namespace MBrace.Aws.Store

open System
open System.Text.RegularExpressions
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization

open Amazon.S3.Model

open MBrace.Core.Internals
open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities

[<AutoOpen>]
module private S3FileStoreImpl =

    let emptyProps : IDictionary<string, obj> = dict []

    let getRandomBucketName() = "/mbrace" + Guid.NewGuid().ToString("N")

    let getObjMetadata (account : AwsAccount) (path : S3Path) = async {
        let req = GetObjectMetadataRequest(BucketName = path.Bucket , Key = path.Key)
        let! ct = Async.CancellationToken
        return! account.S3Client.GetObjectMetadataAsync(req, ct) |> Async.AwaitTaskCorrect
    }

    let enumerateDir (account : AwsAccount) (dirPath : S3Path) map = async {
        let results = ResizeArray<string>()
        let rec aux nextMarker = async {
            let req = ListObjectsRequest(
                        BucketName = dirPath.Bucket,
                        Prefix     = dirPath.Key,
                        Delimiter  = "/",
                        Marker     = nextMarker)
            let! ct = Async.CancellationToken
            let! res = account.S3Client.ListObjectsAsync(req, ct) |> Async.AwaitTaskCorrect
            map res |> results.AddRange
            if res.NextMarker = null then return ()
            else return! aux res.NextMarker
        }

        do! aux null
        return Seq.toArray results
    }
            


///  MBrace File Store implementation that uses Amazon S3 as backend.
[<Sealed; DataContract>]
type S3FileStore private (account : AwsAccount, defaultBucket : string) =

    [<DataMember(Name = "S3Account")>]
    let account = account

    [<DataMember(Name = "DefaultBucket")>]
    let defaultBucket = defaultBucket

    let normalize asDirectory (path : string) =
        match S3Path.TryParse (path, asDirectory = asDirectory) with
        | Some p -> p
        | None -> let cp = S3Path.Combine(defaultBucket, path) in S3Path.Parse(cp, asDirectory = asDirectory)

    let ensureBucketExists (s3p : S3Path) = async {
        let! listed = account.S3Client.ListBucketsAsync() |> Async.AwaitTaskCorrect
        if listed.Buckets |> Seq.exists (fun b -> b.BucketName = s3p.Bucket) |> not then
            let! ct = Async.CancellationToken
            let! _result = account.S3Client.PutBucketAsync(s3p.Bucket, ct) |> Async.AwaitTaskCorrect
            return ()
    }

    /// <summary>
    ///     Creates an MBrace CloudFileStore implementation targeting Amazon S3s
    /// </summary>
    /// <param name="account">AwsAccount to be used.</param>
    /// <param name="defaultBucket">Default S3 Bucket to be used. Will auto-generate name if not specified.</param>
    static member Create(account : AwsAccount, ?defaultBucket : string) =
        let defaultBucket = match defaultBucket with Some b -> b | None -> getRandomBucketName()
        let s3p = S3Path.Parse(S3Path.Combine("/", defaultBucket))
        if not s3p.IsBucket then invalidArg "defaultBucket" <| sprintf "supplied path '%s' is not a valid S3 bucket." defaultBucket
        new S3FileStore(account, s3p.FullPath)

    interface ICloudFileStore with
        member __.Name = "MBrace.Aws.Store.S3FileStore"
        member __.Id = sprintf "Access Key %s, Region %O" account.AccessKey account.Region
        member __.IsCaseSensitiveFileSystem = true
        
        //#region Directory Operations
        
        member __.RootDirectory = "/"

        member __.GetDirectoryName(path : string) = S3Path.GetDirectoryName path

        member __.GetRandomDirectoryName() = getRandomBucketName()

        member __.DirectoryExists(directory : string) = async {
            let s3Path = normalize true directory
            if s3Path.IsRoot then return true
            elif not <| account.S3Client.DoesS3BucketExist s3Path.Bucket then return false
            elif s3Path.IsBucket then return true
            else
                let req = ListObjectsRequest(BucketName = s3Path.Bucket, Prefix = s3Path.Key)
                let! ct = Async.CancellationToken
                let! res = account.S3Client.ListObjectsAsync (req, ct) |> Async.AwaitTaskCorrect
                return res.S3Objects.Count > 0
        }

        member __.CreateDirectory(directory : string) = async {
            let s3Path = normalize true directory
            if s3Path.IsRoot then return () else
            do! ensureBucketExists s3Path
            if not <| s3Path.IsBucket then
                let folderKey = S3Path.Combine(s3Path.Key, S3Path.GetFolderName s3Path.Key + "_$folder$")
                let req = PutObjectRequest(BucketName = s3Path.Bucket, Key = folderKey)
                let! ct = Async.CancellationToken
                do! account.S3Client.PutObjectAsync (req, ct)
                    |> Async.AwaitTaskCorrect
                    |> Async.Ignore
        }

        member __.DefaultDirectory = defaultBucket
        
        member this.DeleteDirectory(directory : string, _recursiveDelete : bool) = async {
            let s3p = S3Path.Parse directory
            if s3p.IsRoot then return invalidOp "cannot delete the root folder."

            let! ct = Async.CancellationToken
            let! response = account.S3Client.ListObjectsAsync(s3p.Bucket, prefix = s3p.Key, cancellationToken = ct) |> Async.AwaitTask

            do! response.S3Objects
                |> Seq.map (fun obj -> S3Path.Combine("/", s3p.Bucket, obj.Key))
                |> Seq.map (this :> ICloudFileStore).DeleteFile
                |> Async.Parallel
                |> Async.Ignore

            if s3p.IsBucket then
                let! _ = account.S3Client.DeleteBucketAsync s3p.Bucket |> Async.AwaitTaskCorrect
                return ()
        }

        member __.EnumerateDirectories(directory : string) = async {
            let s3p = normalize true directory
            if s3p.IsRoot then 
                let! ct = Async.CancellationToken
                let! listed = account.S3Client.ListBucketsAsync(ct) |> Async.AwaitTaskCorrect
                return listed.Buckets |> Seq.map (fun b -> "/" + b.BucketName) |> Seq.toArray
            else
                return! enumerateDir account s3p (fun res -> res.CommonPrefixes |> Seq.map (fun p -> S3Path.Combine("/", s3p.Bucket, p)))
        }

        member __.EnumerateFiles(directory : string) = async {
            let s3p = normalize true directory
            if s3p.IsRoot then return [||] else

            let map (res : ListObjectsResponse) =
                res.S3Objects 
                |> Seq.filter (fun obj -> not (String.IsNullOrEmpty obj.Key || obj.Key.EndsWith "/"))
                |> Seq.map (fun obj -> S3Path.Combine("/", s3p.Bucket, obj.Key))

            return! enumerateDir account s3p map
        }

        //#endregion

        //#region File Operations

        member __.GetFileName(path : string) = S3Path.GetFileName(path)

        member __.DeleteFile(path : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            let req = DeleteObjectRequest(BucketName = s3p.Bucket, Key = s3p.Key)
            let! ct = Async.CancellationToken
            do! account.S3Client.DeleteObjectAsync(req, ct) 
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }
        
        member __.DownloadToLocalFile(cloudSourcePath : string, localTargetPath : string) = async {
            let s3p = normalize false cloudSourcePath
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." cloudSourcePath
            let! ct = Async.CancellationToken
            do! 
                account.S3Client.DownloadToFilePathAsync(s3p.Bucket, s3p.Key, localTargetPath, emptyProps, ct)
                |> Async.AwaitTaskCorrect
        }

        member __.DownloadToStream(cloudSourcePath : string, stream : Stream) = async {
            let s3p = normalize false cloudSourcePath
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." cloudSourcePath
            let! ct = Async.CancellationToken
            let! objStream = 
                account.S3Client.GetObjectStreamAsync(s3p.Bucket, s3p.Key, emptyProps, ct)
                |> Async.AwaitTaskCorrect

            do! objStream.CopyToAsync(stream) |> Async.AwaitTaskCorrect
        }

        member this.FileExists(path : string) = async {
            let! etag = (this :> ICloudFileStore).TryGetETag(path)
            return etag.IsSome
        }
        
        member __.GetFileSize(path : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            let! res = getObjMetadata account s3p
            return res.ContentLength
        }

        member __.GetLastModifiedTime(path : string, _isDirectory : bool) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then raise <| new FileNotFoundException()
            let! res = getObjMetadata account s3p
            return DateTimeOffset(res.LastModified) // returns UTC datetime kind, so wrapping is safe here
        }
                
        member __.IsPathRooted(path : string) = S3Path.TryParse path |> Option.isSome
        
        member __.ReadETag(path : string, etag : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            let props = dict [("IfMatch", box etag)]

            let! ct = Async.CancellationToken
            let! res = 
                account.S3Client.GetObjectStreamAsync(s3p.Bucket, path, props, ct) 
                |> Async.AwaitTaskCorrect
                |> Async.Catch

            match res with
            | Choice1Of2 res -> return Some res
            | _ -> return None
        }
        
        member __.TryGetETag(path : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            let! res = getObjMetadata account s3p |> Async.Catch
            match res with
            | Choice1Of2 res -> return Some res.ETag
            | _ -> return None
        }

        member __.UploadFromLocalFile(localSourcePath : string, cloudTargetPath : string) = async {
            let s3p = normalize false cloudTargetPath
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." cloudTargetPath
            do! ensureBucketExists s3p
            let! ct = Async.CancellationToken
            do! 
                account.S3Client.UploadObjectFromFilePathAsync(s3p.Bucket, s3p.Key, localSourcePath, emptyProps, ct)
                |> Async.AwaitTaskCorrect
        }

        member __.UploadFromStream(cloudTargetPath : string, stream : Stream) = async {
            let s3p = normalize false cloudTargetPath
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." cloudTargetPath
            do! ensureBucketExists s3p
            let! ct = Async.CancellationToken
            do! 
                account.S3Client.UploadObjectFromStreamAsync(s3p.Bucket, s3p.Key, stream, emptyProps, ct)
                |> Async.AwaitTaskCorrect
        }

        member this.WriteETag(path : string, writer : Stream -> Async<'T>) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            do! ensureBucketExists s3p
            let! metaRes = getObjMetadata account s3p // hmmmm
            let! result = async {
                use! stream = account.S3Client.GetObjectWriteStreamAsync(s3p.Bucket, s3p.Key, timeout = TimeSpan.FromMinutes(40.))
                return! writer stream
            }
            
            return metaRes.ETag, result
        }
        
        //#endregion

        member __.Combine(paths) = S3Path.Combine paths

        member __.BeginRead(path : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            return! 
                account.S3Client.GetObjectStreamAsync(s3p.Bucket, s3p.Key, emptyProps) 
                |> Async.AwaitTaskCorrect
        }

        member __.BeginWrite(path : string) = async {
            let s3p = normalize false path
            if not <| s3p.IsObject then invalidArg "path" <| sprintf "path '%s' is not a valid S3 object." path
            do! ensureBucketExists s3p
            return! account.S3Client.GetObjectWriteStreamAsync(s3p.Bucket, s3p.Key, timeout = TimeSpan.FromMinutes(40.))
        }

        member __.WithDefaultDirectory(directory : string) = S3FileStore.Create(account, directory) :> _