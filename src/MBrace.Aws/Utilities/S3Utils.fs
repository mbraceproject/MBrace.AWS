namespace MBrace.Aws.Runtime.Utilities

open System
open System.Net
open System.IO
open System.Threading
open System.Threading.Tasks
open System.Text.RegularExpressions

open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Aws.Runtime

open Amazon.S3
open Amazon.S3.Model

[<AutoOpen>]
module S3Utils =

    let private s3Regex = new Regex("^\s*/([^/]*)/?(.*)", RegexOptions.Compiled)
    let private s3UriRegex = new Regex("^\s*s3://([^/]+)/?(.*)", RegexOptions.Compiled)
    let private s3ArnRegex = new Regex("^\s*arn:aws:s3:::([^/]+)/?(.*)", RegexOptions.Compiled)
    let private directoryNameRegex = new Regex("^(.*)[^/]+$", RegexOptions.Compiled ||| RegexOptions.RightToLeft)
    let private fileNameRegex = new Regex("[^/]*$", RegexOptions.Compiled)
    let private lastFolderRegex = new Regex("([^/]*)/[^/]*$", RegexOptions.Compiled)

    type S3Path = { Bucket : string ; Key : string }
    with
        member p.FullPath = 
            if String.IsNullOrEmpty p.Key then "/" + p.Bucket
            else
                sprintf "/%s/%s" p.Bucket p.Key

        member p.IsBucket = String.IsNullOrEmpty p.Key
        member p.IsRoot = String.IsNullOrEmpty p.Bucket
        member p.IsObject = not(p.IsRoot || p.IsBucket)

        static member TryParse (path : string, ?forceKeyNameGuidelines : bool, ?asDirectory : bool) =
            let forceKeyNameGuidelines = defaultArg forceKeyNameGuidelines false
            let asDirectory = defaultArg asDirectory false
            let inline extractResult (m : Match) =
                let bucketName = m.Groups.[1].Value
                let keyName = m.Groups.[2].Value
                if not (String.IsNullOrEmpty bucketName || Validate.tryBucketName bucketName) then None
                elif not (String.IsNullOrEmpty keyName || Validate.tryKeyName forceKeyNameGuidelines keyName) then None
                else
                    Some { Bucket = bucketName ; Key = if asDirectory && keyName <> "" && not <| keyName.EndsWith "/" then keyName + "/" else keyName  }
                
            let m = s3Regex.Match path
            if m.Success then extractResult m else
            
            let m = s3UriRegex.Match path
            if m.Success then extractResult m else

            let m = s3ArnRegex.Match path
            if m.Success then extractResult m else

            None

        static member Parse(path : string, ?forceKeyNameGuidelines : bool, ?asDirectory : bool) : S3Path =
            let forceKeyNameGuidelines = defaultArg forceKeyNameGuidelines false
            let asDirectory = defaultArg asDirectory false
            let inline extractResult (m : Match) =
                let bucketName = m.Groups.[1].Value
                let keyName = m.Groups.[2].Value
                if not <| String.IsNullOrEmpty bucketName then Validate.bucketName bucketName
                if not <| String.IsNullOrEmpty keyName then Validate.keyName forceKeyNameGuidelines keyName
                { Bucket = bucketName ; Key = if asDirectory && keyName <> "" && not <| keyName.EndsWith "/" then keyName + "/" else keyName }

            let m = s3Regex.Match path
            if m.Success then extractResult m else
            
            let m = s3UriRegex.Match path
            if m.Success then extractResult m else

            let m = s3ArnRegex.Match path
            if m.Success then extractResult m else

            invalidArg "path" <| sprintf "Invalid S3 path format '%s'." path

        static member Combine([<ParamArray>]paths : string []) = 
            let acc = new ResizeArray<string>()
            for path in paths do
                if path.StartsWith "/" || path.StartsWith "s3://" || path.StartsWith "arn:aws:s3:::" then acc.Clear()
                elif acc.Count > 0 && not <| acc.[acc.Count - 1].EndsWith "/" then acc.Add "/"
                acc.Add path

            String.concat "" acc

        static member Normalize(path : string) = S3Path.Parse(path).FullPath

        static member GetDirectoryName(path : string) = 
            let m = directoryNameRegex.Match path
            if m.Success then m.Groups.[1].Value
            elif path.Contains "/" then path
            else ""

        static member GetFileName(path : string) =
            let m = fileNameRegex.Match path
            if m.Success then m.Groups.[0].Value else ""

        static member GetFolderName(path : string) =
            let m = lastFolderRegex.Match path
            if m.Success then m.Groups.[1].Value else ""


    [<Sealed; AutoSerializable(false)>]
    type private S3WriteStream (client : IAmazonS3, bucketName : string, key : string, uploadId : string, timeout : TimeSpan option) =
        inherit Stream()

        static let bufSize = 5 * 1024 * 1024 // 5 MiB : the minimum upload size per non-terminal chunk permited by Amazon
        static let bufPool = System.ServiceModel.Channels.BufferManager.CreateBufferManager(256L, bufSize)

        let cts = new CancellationTokenSource()
        let mutable isClosed = false
        let mutable position = 0L
        let mutable i = 0
        let buffer = bufPool.TakeBuffer(bufSize)
        let uploads = new ResizeArray<Task<UploadPartResponse>>()

        let checkClosed() = if isClosed then raise <| new ObjectDisposedException("S3WriteStream")

        let upload (bytes : byte []) (offset : int) (count : int) =
            let request = 
                new UploadPartRequest(
                    BucketName = bucketName, 
                    Key = key, 
                    UploadId = uploadId, 
                    PartNumber = uploads.Count + 1, 
                    InputStream = new MemoryStream(bytes, offset, count))

            let task = client.UploadPartAsync(request, cts.Token)
            uploads.Add(task)

        let flush () =
            if i > 0 then
                upload buffer 0 i
                i <- 0

        let close () = async {
            isClosed <- true
            flush()
            if uploads.Count = 0 then upload buffer 0 0
            let! results = uploads |> Task.WhenAll |> Async.AwaitTaskCorrect
            bufPool.ReturnBuffer buffer
            let partETags = results |> Seq.map (fun r -> new PartETag(r.PartNumber, r.ETag))
            let request = 
                new CompleteMultipartUploadRequest(
                    BucketName = bucketName,
                    Key = key,
                    UploadId = uploadId,
                    PartETags = new ResizeArray<_>(partETags))

            let! _ = Async.AwaitTaskCorrect <| client.CompleteMultipartUploadAsync(request, cts.Token)
            return ()
        }

        do 
            match timeout with
            | None -> ()
            | Some t ->
                let _ = cts.Token.Register(fun () -> client.AbortMultipartUploadAsync(bucketName, key, uploadId) |> ignore)
                cts.CancelAfter t

        override __.CanRead    = false
        override __.CanSeek    = false
        override __.CanWrite   = true
        override __.CanTimeout = true

        override __.Length = position
        override __.Position 
            with get () = position
            and  set _  = raise <| NotSupportedException()

        override __.SetLength _ = raise <| NotSupportedException()
        override __.Seek (_, _) = raise <| NotSupportedException()
        override __.Read (_, _, _) = raise <| NotSupportedException()

        override __.Write (source : byte [], offset : int, count : int) =
            checkClosed()
            if offset < 0 || count < 0 || offset + count > source.Length then raise <| ArgumentOutOfRangeException()
            elif i + count < bufSize then
                Buffer.BlockCopy(source, offset, buffer, i, count)
                i <- i + count
            elif i = 0 then
                upload source offset count
            else
                let k = bufSize - i
                Buffer.BlockCopy(source, offset, buffer, i, k)
                i <- bufSize
                flush()
                if count - k < bufSize then
                    Buffer.BlockCopy(source, offset + k, buffer, 0, count - k)
                    i <- count - k
                else
                    upload source (offset + k) (count - k)

            position <- position + int64 (count - offset)
            
        override __.Flush() = ()
        override __.Close() = if not isClosed then Async.RunSynchronously(close(), cancellationToken = cts.Token)


    type IAmazonS3 with

        /// <summary>
        ///     Asynchronously gets an object write stream for given uri in S3 storage
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="key"></param>
        /// <param name="timeout"></param>
        member s3.GetObjectWriteStreamAsync(bucketName : string, key : string, ?timeout : TimeSpan) : Async<Stream> = async {
            let request = new InitiateMultipartUploadRequest(BucketName = bucketName, Key = key)
            let! response = s3.InitiateMultipartUploadAsync(request) |> Async.AwaitTaskCorrect
            return new S3WriteStream(s3, bucketName, key, response.UploadId, timeout) :> Stream
        }