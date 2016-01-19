namespace MBrace.Aws.Tests

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

    [<Test>]
    member __.``S3 Stream Writer upload large file`` () =
        let largeBuf = Array.init (1024 * 1024) byte
        let file = s3Store.GetRandomFilePath s3Store.DefaultDirectory
        try
            let stream = s3Store.BeginWrite file |> Async.RunSync
            for i in 1 .. 17 do stream.Write(largeBuf, 0, 1024 * 1024)
            stream.Close()
            s3Store.GetFileSize file |> Async.RunSync |> shouldEqual (17L * 1024L * 1024L)
        finally
            s3Store.DeleteFile file |> Async.RunSync

    override __.FileStore = s3Store
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = true
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf