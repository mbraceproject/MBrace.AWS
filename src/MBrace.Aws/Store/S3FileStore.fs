namespace MBrace.Aws.Store

open System
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization

open Amazon.S3.Model

open MBrace.Core.Internals
open MBrace.Aws.Runtime

//[<Sealed>]
//type internal S3WriteStream () =
//    inherit Stream()
//
//    let inner = new MemoryStream()
//
//    override __.CanRead    = false
//    override __.CanSeek    = false
//    override __.CanWrite   = true
//    override __.CanTimeout = true
//
//    override __.Length = inner.Length
//    override __.Position 
//        with get () = inner.Position 
//        and  set x  = inner.Position <- x
//
//    override __.SetLength _ = raise <| NotSupportedException()
//    override __.Seek (_, _) = raise <| NotSupportedException()
//    override __.Read (_, _, _) = raise <| NotSupportedException()
//    override __.Write (buffer, offset, count) = inner.Write(buffer, offset, count)
//    override __.Flush() = inner.Flush()

///  MBrace File Store implementation that uses Amazon S3 as backend.
[<Sealed; DataContract>]
type S3FileStore private 
        (account    : AwsS3Account, 
         bucketName : string, 
         defaultDir : string) =
    [<DataMember(Name = "S3Account")>]
    let account = account
    
    [<DataMember(Name = "BucketName")>]
    let bucketName = bucketName

    [<DataMember(Name = "DefaultDir")>]
    let defaultDir = defaultDir

    let normalizeDirPath (dir : string) =
        if dir.EndsWith "/" then dir else dir + "/"
    
    let getObjMetadata path = async {
        let req = GetObjectMetadataRequest(BucketName = bucketName, Key = path)
        let! ct = Async.CancellationToken
        return! account.S3Client.GetObjectMetadataAsync(req, ct)
                |> Async.AwaitTaskCorrect
    }

    let listObjects prefix nextMarker = async {
        let req = ListObjectsRequest(
                    BucketName = bucketName,
                    Prefix     = prefix,
                    Delimiter  = "/",
                    Marker     = nextMarker)
        let! ct = Async.CancellationToken
        return! account.S3Client.ListObjectsAsync(req, ct) 
                |> Async.AwaitTaskCorrect
    }

    let enumerateDir directory map = async {
            let prefix  = normalizeDirPath directory
            let results = ResizeArray<string>()

            let rec aux nextMarker = async {
                let! res = listObjects prefix nextMarker
                map res |> results.AddRange
                if res.NextMarker = null then return ()
                else return! aux res.NextMarker
            }

            do! aux null
            return Seq.toArray results
        }

    interface ICloudFileStore with
        member __.Name = "MBrace.Aws.Store.S3FileStore"
        member __.Id = sprintf "arn:aws:s3::%s" bucketName
        member __.IsCaseSensitiveFileSystem = false
        
        //#region Directory Operations
        
        member __.RootDirectory = "/"

        member __.GetDirectoryName(path) = Path.GetDirectoryName path

        member __.GetRandomDirectoryName() = Guid.NewGuid().ToString()

        member __.DirectoryExists(directory) = async {
            let prefix = normalizeDirPath directory
            let req = ListObjectsRequest(
                        BucketName = bucketName, 
                        Prefix = prefix)
            let! ct = Async.CancellationToken
            let! res = account.S3Client.ListObjectsAsync (req,ct)
                       |> Async.AwaitTaskCorrect
            return res.S3Objects.Count > 0
        }

        member __.CreateDirectory(directory) = async {
            let key = normalizeDirPath directory
            let req = PutObjectRequest(BucketName = bucketName, Key = key)
            let! ct = Async.CancellationToken
            do! account.S3Client.PutObjectAsync (req, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.DefaultDirectory = defaultDir
        
        member this.DeleteDirectory(directory, _recursiveDelete) = async {
            let! paths = (this :> ICloudFileStore).EnumerateFiles(directory)

            do! paths 
                |> Seq.map (this :> ICloudFileStore).DeleteFile
                |> Async.Parallel
                |> Async.Ignore
        }

        member __.EnumerateDirectories(directory) = 
            enumerateDir directory (fun res -> res.CommonPrefixes)

        member __.EnumerateFiles(directory) =
            let map = fun (res : ListObjectsResponse) -> 
                res.S3Objects 
                |> Seq.filter (fun obj -> not <| obj.Key.EndsWith "/")
                |> Seq.map (fun obj -> obj.Key)
            enumerateDir directory map

        //#endregion

        //#region File Operations

        member __.GetFileName(path) = Path.GetFileName(path)

        member __.DeleteFile(path) = async {
            let req = DeleteObjectRequest(BucketName = bucketName, Key = path)
            let! ct = Async.CancellationToken
            do! account.S3Client.DeleteObjectAsync(req, ct) 
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }
        
        member __.DownloadToLocalFile(cloudSourcePath, localTargetPath) = async {
            let! ct = Async.CancellationToken
            do! account.S3Client.DownloadToFilePathAsync(
                    bucketName, 
                    cloudSourcePath, 
                    localTargetPath, 
                    Dictionary<string, obj>(),
                    ct)
                |> Async.AwaitTaskCorrect
        }

        member __.DownloadToStream(path, stream) = async {
            let! ct = Async.CancellationToken
            let! objStream = 
                account.S3Client.GetObjectStreamAsync(
                    bucketName, path, Dictionary<string, obj>(), ct)
                |> Async.AwaitTaskCorrect

            do! objStream.CopyToAsync(stream)
                |> Async.AwaitTaskCorrect
        }

        member this.FileExists(path) = async {
            let! etag = (this :> ICloudFileStore).TryGetETag(path)
            return etag.IsSome
        }
        
        member __.GetFileSize(path) = async {
            let! res = getObjMetadata path
            return res.ContentLength
        }

        member __.GetLastModifiedTime(path, isDirectory) = 
            failwith "Not implemented yet"
                
        member __.IsPathRooted(path) = path.Contains "/" |> not
        
        member __.ReadETag(path, etag) = async {
            let props = Dictionary<string, obj>()
            props.["IfMatch"] <- etag

            let! ct = Async.CancellationToken
            let! res = 
                account.S3Client.GetObjectStreamAsync(
                    bucketName, 
                    path, 
                    props,
                    ct) 
                |> Async.AwaitTaskCorrect
                |> Async.Catch

            match res with
            | Choice1Of2 res -> return Some res
            | _ -> return None
        }
        
        member __.TryGetETag(path) = async {
            let! res = getObjMetadata path |> Async.Catch
            match res with
            | Choice1Of2 res -> return Some res.ETag
            | _ -> return None
        }

        member __.UploadFromLocalFile(localSourcePath, cloudTargetPath) = async {
            let! ct = Async.CancellationToken
            do! account.S3Client.UploadObjectFromFilePathAsync(
                    bucketName, 
                    cloudTargetPath, 
                    localSourcePath, 
                    Dictionary<string, obj>(),
                    ct)
                |> Async.AwaitTaskCorrect
        }

        member __.UploadFromStream(path, stream) = async {
            let! ct = Async.CancellationToken
            do! account.S3Client.UploadObjectFromStreamAsync(
                    bucketName,
                    path,
                    stream,
                    Dictionary<string, obj>(),
                    ct)
                |> Async.AwaitTaskCorrect
        }

        member this.WriteETag(path, writer) = async {
            let! metaRes = getObjMetadata path
            let! result = async {
                use stream = new MemoryStream()
                let! result = writer(stream)
                do! (this :> ICloudFileStore).UploadFromStream(path, stream)
                return result
            }

            return metaRes.ETag, result
        }
        
        //#endregion

        member __.Combine(paths) = Path.Combine paths

        member __.BeginRead(path) = async {
            return! account.S3Client.GetObjectStreamAsync(
                        bucketName, 
                        path, 
                        Dictionary<string, obj>()) 
                    |> Async.AwaitTaskCorrect
        }

        member __.BeginWrite(path) = failwith "Not implemented yet"

        member __.WithDefaultDirectory(directory) = 
            new S3FileStore(account, bucketName, directory) :> _