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

    let account = getAWSTestAccount()

    let tablePrefix = sprintf "testmbrace-%s" <| System.Guid.NewGuid().ToString("N")
    let ddbAtomProvider = DynamoDBAtomProvider.Create(account, tablePrefix = tablePrefix)
    let serializer = new FsPicklerBinarySerializer(useVagabond = false)
    let imem = ThreadPoolRuntime.Create(atomProvider = ddbAtomProvider, serializer = serializer, memoryEmulation = MemoryEmulation.Shared)

    let run x = Async.RunSync x

    [<TestFixtureTearDown>]
    member __.``Clean up leftover buckets``() =
        ddbAtomProvider.ClearTablesAsync() |> run

    override __.Run(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.RunLocally(wf : Cloud<'T>) = imem.RunSynchronously wf
    override __.IsSupportedNamedLookup = true
    override __.Repeats = 2