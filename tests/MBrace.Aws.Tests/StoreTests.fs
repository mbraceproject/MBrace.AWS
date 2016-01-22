namespace MBrace.AWS.Tests

open System.IO
open NUnit.Framework

open Amazon

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Store

[<TestFixture>]
type ``Local S3 FileStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 100)

    let account = getAWSTestAccount()

    let bucketPrefix = sprintf "testmbrace%04x" <| System.Random().Next(int System.UInt16.MaxValue)
    let s3store = S3FileStore.Create(account, bucketPrefix = bucketPrefix)
    let store = s3store :> ICloudFileStore
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer, memoryEmulation = MemoryEmulation.Shared)

    let run x = Async.RunSync x

    [<TestFixtureTearDown>]
    member __.``Clean up leftover buckets``() =
        s3store.ClearBucketsAsync() |> run


    [<Test>]
    member __.``S3 Stream Writer upload large file`` () =
        let largeBuf = Array.init (1024 * 1024) byte
        let file = store.GetRandomFilePath store.DefaultDirectory
        try
            let stream = store.BeginWrite file |> run
            for i in 1 .. 17 do stream.Write(largeBuf, 0, 1024 * 1024)
            stream.Close()
            store.GetFileSize file |> run |> shouldEqual (17L * 1024L * 1024L)
        finally
            store.DeleteFile file |> run


    [<Test>]
    member __.``S3 Stream Reader should be seekable`` () =
        let file = store.GetRandomFilePath store.DefaultDirectory
        do
            use stream = store.BeginWrite file |> run
            for i in 0 .. 99 do stream.WriteByte (byte i)

        try
            use stream = store.BeginRead file |> run
            stream.Length |> shouldEqual 100L
            stream.Position |> shouldEqual 0L
            stream.ReadByte() |> shouldEqual 0
            stream.Position |> shouldEqual 1L

            stream.Seek(50L, SeekOrigin.Begin) |> shouldEqual 50L
            stream.Position |> shouldEqual 50L
            stream.ReadByte() |> shouldEqual 50
            stream.Position |> shouldEqual 51L

            stream.Seek(9L, SeekOrigin.Current) |> shouldEqual 60L
            stream.Position |> shouldEqual 60L
            stream.ReadByte() |> shouldEqual 60
            stream.Position |> shouldEqual 61L

        finally
            store.DeleteFile file |> run


    override __.FileStore = store
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = true
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf