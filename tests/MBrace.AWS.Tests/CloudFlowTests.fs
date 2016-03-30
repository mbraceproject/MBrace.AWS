namespace MBrace.AWS.Tests.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Tests

open NUnit.Framework

[<AbstractClass; TestFixture>]
type ``AWS CloudFlow Tests`` (config : Configuration, localWorkers : int) =
    inherit ``CloudFlow tests`` ()
    let session = new ClusterSession(config, localWorkers)

    [<TestFixtureSetUp>]
    member __.Init () = session.Start()

    [<TestFixtureTearDown>]
    member __.Fini () = session.Stop()

    override __.IsSupportedStorageLevel _ = true

    override __.Run (workflow : Cloud<'T>) = 
        session.Cluster.Run(workflow)

    override __.RunLocally(workflow : Cloud<'T>) = 
        session.Cluster.RunLocally(workflow)

    override __.RunWithLogs(workflow : Cloud<unit>) =
        let cloudProcess = session.Cluster.CreateProcess workflow
        do cloudProcess.Result
        cloudProcess.GetLogs () |> Array.map CloudLogEntry.Format

    override __.FsCheckMaxNumberOfTests = 3
    override __.FsCheckMaxNumberOfIOBoundTests = 3


[<Category("Standalone Cluster")>]
type ``CloudFlow Tests - Standalone Cluster - Remote Storage`` () =
    inherit ``AWS CloudFlow Tests``(getMBraceAWSConfig None, 4)