namespace MBrace.Aws.Tests

open System.IO
open NUnit.Framework

open Amazon

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool

open MBrace.Aws
open MBrace.Aws.Runtime
open MBrace.Aws.Store

[<TestFixture>]
type ``Local S3 FileStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 100)

    let account = AwsAccount.Create("Default", RegionEndpoint.EUCentral1)
    let s3Store = S3FileStore.Create(account) :> ICloudFileStore
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(fileStore = s3Store, serializer = serializer, memoryEmulation = MemoryEmulation.Shared)

    let run x = Async.RunSync x

    [<Test>]
    member __.``S3 Stream Writer upload large file`` () =
        let largeBuf = Array.init (1024 * 1024) byte
        let file = s3Store.GetRandomFilePath s3Store.DefaultDirectory
        try
            let stream = s3Store.BeginWrite file |> run
            for i in 1 .. 17 do stream.Write(largeBuf, 0, 1024 * 1024)
            stream.Close()
            s3Store.GetFileSize file |> run |> shouldEqual (17L * 1024L * 1024L)
        finally
            s3Store.DeleteFile file |> run


    [<Test>]
    member __.``S3 Stream Reader should be seekable`` () =
        let file = s3Store.GetRandomFilePath s3Store.DefaultDirectory
        do
            use stream = s3Store.BeginWrite file |> run
            for i in 0 .. 99 do stream.WriteByte (byte i)

        try
            use stream = s3Store.BeginRead file |> run
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
            s3Store.DeleteFile file |> run


    override __.FileStore = s3Store
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = true
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf