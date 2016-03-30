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
type ``Local DynamoDB Atom Tests`` () =
    inherit ``CloudAtom Tests``(parallelismFactor = 5)

    static do init()

    let tablePrefix = sprintf "testmbrace-%s" <| System.Guid.NewGuid().ToString("N")
    let ddbAtomProvider = DynamoDBAtomProvider.Create(getAWSRegion(), getAWSCredentials(), tablePrefix = tablePrefix)
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(atomProvider = ddbAtomProvider, serializer = serializer, memoryEmulation = MemoryEmulation.Copied)

    let run x = Async.RunSync x

    [<TestFixtureTearDown>]
    member __.``Clean up leftover tables``() =
        ddbAtomProvider.ClearTablesAsync() |> run

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedNamedLookup = true
    override __.Repeats = 2

[<AbstractClass; TestFixture>]
type ``Cluster CloudAtom Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudAtom Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsSupportedNamedLookup = true
    override __.Repeats = 2


[<TestFixture; Category("Standalone Cluster")>]
type ``CloudAtom Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Cluster CloudAtom Tests``(getMBraceAWSConfig None, 4)