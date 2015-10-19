namespace MBrace.Aws.Store

open System
open System.IO
open System.Runtime.Serialization

open Amazon.S3

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<Sealed; DataContract>]
type S3FileStore private (account : AwsS3Account) =
    

    interface ICloudFileStore with        
        member x.BeginRead(path) = failwith "Not implemented yet"
        member x.BeginWrite(path) = failwith "Not implemented yet"
        member x.Combine(paths) = failwith "Not implemented yet"
        member x.CreateDirectory(directory) = failwith "Not implemented yet"
        member x.DefaultDirectory = failwith "Not implemented yet"
        member x.DeleteDirectory(directory, recursiveDelete) = failwith "Not implemented yet"
        member x.DeleteFile(path) = failwith "Not implemented yet"
        member x.DirectoryExists(directory) = failwith "Not implemented yet"
        member x.DownloadToLocalFile(cloudSourcePath, localTargetPath) = failwith "Not implemented yet"
        member x.DownloadToStream(path, stream) = failwith "Not implemented yet"
        member x.EnumerateDirectories(directory) = failwith "Not implemented yet"
        member x.EnumerateFiles(directory) = failwith "Not implemented yet"
        member x.FileExists(path) = failwith "Not implemented yet"
        member x.GetDirectoryName(path) = failwith "Not implemented yet"
        member x.GetFileName(path) = failwith "Not implemented yet"
        member x.GetFileSize(path) = failwith "Not implemented yet"
        member x.GetLastModifiedTime(path, isDirectory) = failwith "Not implemented yet"
        member x.GetRandomDirectoryName() = failwith "Not implemented yet"
        member x.Id = failwith "Not implemented yet"
        member x.IsCaseSensitiveFileSystem = failwith "Not implemented yet"
        member x.IsPathRooted(path) = failwith "Not implemented yet"
        member x.Name = failwith "Not implemented yet"
        member x.ReadETag(path, etag) = failwith "Not implemented yet"
        member x.RootDirectory = failwith "Not implemented yet"
        member x.TryGetETag(path) = failwith "Not implemented yet"
        member x.UploadFromLocalFile(localSourcePath, cloudTargetPath) = failwith "Not implemented yet"
        member x.UploadFromStream(path, stream) = failwith "Not implemented yet"
        member x.WithDefaultDirectory(directory) = failwith "Not implemented yet"
        member x.WriteETag(path, writer) = failwith "Not implemented yet"