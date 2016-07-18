namespace MBrace.AWS.Tests.Runtime

open System

open NUnit.Framework

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests
open MBrace.AWS
open MBrace.AWS.Tests

[<AbstractClass; TestFixture>]
type ``AWS Cloud Tests`` (config : Configuration, localWorkers : int) =
    inherit ``Cloud Tests`` (parallelismFactor = 20, delayFactor = 15000)
    let session = new ClusterSession(config, localWorkers)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.Run (workflow : Cloud<'T>) = 
        session.Cluster.RunAsync (workflow)
        |> Async.RunSync

    override __.Run (workflow : ICloudCancellationTokenSource -> #Cloud<'T>) = 
        async {
            let cluster = session.Cluster
            let cts = cluster.CreateCancellationTokenSource()
            try return! cluster.RunAsync(workflow cts, cancellationToken = cts.Token)
            finally cts.Cancel()
        } |> Async.RunSync

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let cloudProcess = session.Cluster.CreateProcess workflow
        do cloudProcess.Result
        cloudProcess.GetLogs () |> Array.map CloudLogEntry.Format

    override __.RunLocally(workflow : Cloud<'T>) = session.Cluster.RunLocally(workflow)

    override __.IsTargetWorkerSupported = true
    override __.IsSiftedWorkflowSupported = true
    override __.FsCheckMaxTests = 4
    override __.Repeats = 1
    override __.UsesSerialization = true

[<Category("Standalone Cluster")>]
type ``Cloud Tests - Standalone Cluster`` () =
    inherit ``AWS Cloud Tests``(getMBraceAWSConfig None, 4)