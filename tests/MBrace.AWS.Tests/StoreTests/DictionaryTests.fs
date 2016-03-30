namespace MBrace.AWS.Tests.Store

open NUnit.Framework
open MBrace.Core
open MBrace.Core.Tests
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool
open MBrace.AWS
open MBrace.AWS.Store
open MBrace.AWS.Tests

[<TestFixture>]
type ``Local DynamoDB Dictionary Tests`` () =
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)

    static do init()

    let tableName = sprintf "testmbrace-%04d" <| System.Random().Next(0, 10000)
    let ddbDictionaryProvider = DynamoDBDictionaryProvider.Create(getAWSRegion(), getAWSCredentials(), tableName = tableName)
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(dictionaryProvider = ddbDictionaryProvider, serializer = serializer, memoryEmulation = MemoryEmulation.Copied)

    let run x = Async.RunSync x

    [<TestFixtureTearDown>]
    member __.``Clean up leftover tables``() =
        ddbDictionaryProvider.DeleteTableAsync() |> run

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedNamedLookup = true
    override __.IsInMemoryFixture = true

[<AbstractClass; TestFixture>]
type ``Cluster CloudDictionary Tests``(config : Configuration, localWorkers : int) = 
    inherit ``CloudDictionary Tests``(parallelismFactor = 5)
    let session = new ClusterSession(config, localWorkers)
    
    [<TestFixtureSetUp>]
    member __.Init() = session.Start()
    
    [<TestFixtureTearDown>]
    member __.Fini() = session.Stop()
    
    override __.Run wf = session.Cluster.Run wf
    override __.RunLocally wf = session.Cluster.RunLocally wf
    override __.IsInMemoryFixture = false
    override __.IsSupportedNamedLookup = true


[<TestFixture; Category("Standalone Cluster")>]
type ``CloudDictionary Tests - Standalone Cluster - Remote Storage``() = 
    inherit ``Cluster CloudDictionary Tests``(getMBraceAWSConfig None, 4)