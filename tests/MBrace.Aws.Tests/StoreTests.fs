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
    let s3Store = S3FileStore.Create(account)
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(fileStore = s3Store, serializer = serializer, memoryEmulation = MemoryEmulation.Shared)

    override __.FileStore = s3Store :> _
    override __.Serializer = serializer :> _
    override __.IsCaseSensitive = true
    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf