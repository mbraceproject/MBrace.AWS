namespace MBrace.Aws.Store

open System
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization

open Amazon.S3
open Amazon.S3.Model

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<AutoOpen>]
module private S3Utils =
    let normalizeDirPath (dir : string) =
        if dir.EndsWith "/" then dir else dir + "/"

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

//    let enumerateFiles directory = async {
//        let 
//    }

    interface ICloudFileStore with
        member __.Name = "MBrace.Aws.Store.S3FileStore"
        member __.Id = sprintf "arn:aws:s3::%s" bucketName
        member __.IsCaseSensitiveFileSystem = false

        member __.BeginRead(path) = async {
            return! 
                account.S3Client.GetObjectStreamAsync(
                    bucketName, 
                    path, 
                    Dictionary<string, obj>()) 
                |> Async.AwaitTaskCorrect
        }

        member __.BeginWrite(path) = failwith "Not implemented yet"

        //#region Directory Operations
        
        member __.RootDirectory = "/"

        member __.GetDirectoryName(path) = Path.GetDirectoryName path

        member __.GetRandomDirectoryName() = Guid.NewGuid().ToString()

        member __.DirectoryExists(directory) = async {
            let prefix = normalizeDirPath directory
            let req = ListObjectsRequest(
                        BucketName = bucketName, 
                        Prefix = prefix)
            let! res = account.S3Client.ListObjectsAsync req
                       |> Async.AwaitTaskCorrect
            return res.S3Objects.Count > 0
        }

        member __.CreateDirectory(directory) = async {
            let key = normalizeDirPath directory
            let req = PutObjectRequest(BucketName = bucketName, Key = key)
            do! account.S3Client.PutObjectAsync req
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.DefaultDirectory = defaultDir
        member this.DeleteDirectory(directory, recursiveDelete) = async {
            
//            let req = DeleteObjectsRequest(BucketName = bucketName)
//            req.Objects.Add(new KeyVersion())
//
//            // TODO : handle partial failures
//            do! account.S3Client.DeleteObjectsAsync(req) 
//                |> Async.AwaitTaskCorrect
//                |> Async.Ignore
        }

        //#endregion

        member __.Combine(paths) = Path.Combine paths

        member __.DeleteFile(path) = async {
            let req = DeleteObjectRequest(BucketName = bucketName, Key = path)
            do! account.S3Client.DeleteObjectAsync(req) 
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }
        
        member __.DownloadToLocalFile(cloudSourcePath, localTargetPath) = failwith "Not implemented yet"
        member __.DownloadToStream(path, stream) = failwith "Not implemented yet"
        member __.EnumerateDirectories(directory) = failwith "Not implemented yet"
        member __.EnumerateFiles(directory) = failwith "Not implemented yet"
        member __.FileExists(path) = failwith "Not implemented yet"        
        member __.GetFileName(path) = Path.GetFileName(path)
        member __.GetFileSize(path) = failwith "Not implemented yet"
        member __.GetLastModifiedTime(path, isDirectory) = failwith "Not implemented yet"
                
        member x.IsPathRooted(path) = path.Contains "/" |> not
        
        member x.ReadETag(path, etag) = failwith "Not implemented yet"
        
        member x.TryGetETag(path) = failwith "Not implemented yet"
        member x.UploadFromLocalFile(localSourcePath, cloudTargetPath) = failwith "Not implemented yet"
        member x.UploadFromStream(path, stream) = failwith "Not implemented yet"        
        member x.WriteETag(path, writer) = failwith "Not implemented yet"

        member __.WithDefaultDirectory(directory) = 
            new S3FileStore(account, bucketName, directory) :> _