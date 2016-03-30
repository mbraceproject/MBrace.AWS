namespace MBrace.AWS.Tests.Store

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
open MBrace.AWS.Tests

[<TestFixture>]
type ``Local SQS Queue Tests`` () =
    inherit ``CloudQueue Tests``(parallelismFactor = 10)

    static do init()

    let queuePrefix = sprintf "testmbrace-%s" <| System.Guid.NewGuid().ToString("N")
    let sqsQueueProvider = SQSCloudQueueProvider.Create(getAWSRegion(), getAWSCredentials(), queuePrefix = queuePrefix)
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(queueProvider = sqsQueueProvider, serializer = serializer, memoryEmulation = MemoryEmulation.Copied)

    let run x = Async.RunSync x

    [<TestFixtureTearDown>]
    member __.``Clean up leftover buckets``() =
        sqsQueueProvider.ClearQueuesAsync() |> run

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedNamedLookup = true

[<AbstractClass; TestFixture>]
type ``Cluster CloudQueue Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudQueue Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsSupportedNamedLookup = true


[<TestFixture; Category("Standalone Cluster")>]
type ``CloudQueue Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Cluster CloudQueue Tests``(getMBraceAWSConfig None, 4)