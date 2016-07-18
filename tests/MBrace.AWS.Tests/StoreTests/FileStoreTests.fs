namespace MBrace.AWS.Tests.Store

open System.IO

open Swensen.Unquote.Assertions
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
open MBrace.AWS.Tests

[<TestFixture>]
type ``Local S3 FileStore Tests`` () =
    inherit ``CloudFileStore Tests``(parallelismFactor = 20)

    static do init()

    let bucketPrefix = sprintf "testmbrace%04x" <| System.Random().Next(int System.UInt16.MaxValue)
    let s3store = S3FileStore.Create(getAWSRegion(), getAWSCredentials(), bucketPrefix = bucketPrefix)
    let store = s3store :> ICloudFileStore
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(fileStore = store, serializer = serializer, memoryEmulation = MemoryEmulation.Copied)

    let run x = Async.RunSync x

    override __.FileStore = store
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = true
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf

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
            test <@ store.GetFileSize file |> run = 17L * 1024L * 1024L @>
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
            test <@ stream.Length = 100L @>
            test <@ stream.Position = 0L @>
            test <@ stream.ReadByte() = 0 @>
            test <@ stream.Position = 1L @>

            test <@ stream.Seek(50L, SeekOrigin.Begin) = 50L @>
            test <@ stream.Position = 50L @>
            test <@ stream.ReadByte() = 50 @>
            test <@ stream.Position = 51L @>

            test <@ stream.Seek(9L, SeekOrigin.Current) = 60L @>
            test <@ stream.Position = 60L @>
            test <@ stream.ReadByte() = 60 @>
            test <@ stream.Position = 61L @>

        finally
            store.DeleteFile file |> run

[<AbstractClass; TestFixture>]
type ``Cluster CloudFileStore Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudFileStore Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.FileStore = session.Cluster.Store.CloudFileSystem.Store
    override __.Serializer = session.Cluster.Store.Serializer.Serializer
    override __.IsCaseSensitive = true


[<TestFixture; Category("Standalone Cluster")>]
type ``CloudFileStore Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Cluster CloudFileStore Tests``(getMBraceAWSConfig None, 4)