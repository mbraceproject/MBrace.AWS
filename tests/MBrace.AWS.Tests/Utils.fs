namespace MBrace.AWS.Tests

open System
open System.IO
open System.Threading
open NUnit.Framework

open Amazon
open Amazon.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components
open MBrace.ThreadPool

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS.Store

#nowarn "445"

[<AutoOpen>]
module Utils =

    let init() = ProcessConfiguration.InitAsClient()
        
    let getEnvironmentVariableOrDefault envName defaultValue = 
        match Environment.ResolveEnvironmentVariable envName with
        | null | "" -> defaultValue
        | ev -> ev

    let getAWSRegion () = 
        match Environment.ResolveEnvironmentVariable "AWS_REGION" with
        | null | "" -> AWSRegion.EUCentral1
        | region -> AWSRegion.Parse region

    let getAWSProfileName () = getEnvironmentVariableOrDefault "AWS_CREDENTIAL_STORE_PROFILE" "default"
    let getAWSCredentials () =
        try MBraceAWSCredentials.FromEnvironmentVariables() 
        with _ -> MBraceAWSCredentials.FromCredentialsStore(profileName = getAWSProfileName())

    let getMBraceAWSConfig predixId =
        let prefixId = 
            match predixId with
            | None -> sprintf "tests%04d" <| Random().Next(0, 10000)
            | Some id -> id

        Configuration.Define(getAWSRegion(), getAWSCredentials(), prefixId)


    type AWSCluster with
        member cluster.NukeS3BucketsAsync(?bucketNameFilter : string -> bool) = async {
            let bucketNameFilter = defaultArg bucketNameFilter (fun b -> b.ToLower().StartsWith "mbrace")
            let! ct = Async.CancellationToken
            let! response = cluster.S3Client.ListBucketsAsync(ct) |> Async.AwaitTaskCorrect
            do! response.Buckets 
                |> Seq.filter (fun b -> bucketNameFilter b.BucketName)
                |> Seq.map (fun b -> cluster.S3Client.DeleteBucketAsyncSafe(b.BucketName))
                |> Async.Parallel
                |> Async.Ignore
        }

        member cluster.NukeDynamoDBTablesAsync(?tableNameFilter : string -> bool) = async {
            let tableNameFilter = defaultArg tableNameFilter (fun tn -> tn.ToLower().StartsWith "mbrace")
            let! ct = Async.CancellationToken
            let! response = cluster.DynamoDBClient.ListTablesAsync(ct) |> Async.AwaitTaskCorrect
            do! response.TableNames
                |> Seq.filter tableNameFilter
                |> Seq.map cluster.DynamoDBClient.DeleteTableAsyncSafe
                |> Async.Parallel
                |> Async.Ignore
        }

        member cluster.NukeSqsQueuesAsync(?queueNameFilter : string -> bool) = async {
            let queueNameFilter = defaultArg queueNameFilter (fun qn -> qn.ToLower().StartsWith "mbrace")
            let! ct = Async.CancellationToken
            let! response = cluster.SQSClient.ListQueuesAsync("", ct) |> Async.AwaitTaskCorrect
            do!
                response.QueueUrls 
                |> Seq.filter (fun uri -> uri.Split('/') |> Array.last |> queueNameFilter)
                |> Seq.map (fun uri -> cluster.SQSClient.DeleteQueueUri uri)
                |> Async.Parallel
                |> Async.Ignore
        }
            
        member cluster.NukeS3Buckets(?bucketNameFilter : string -> bool) = 
            cluster.NukeS3BucketsAsync(?bucketNameFilter = bucketNameFilter) |> Async.RunSync

        member cluster.NukeDynamoDBTables(?tableNameFilter : string -> bool) =
            cluster.NukeDynamoDBTablesAsync(?tableNameFilter = tableNameFilter) |> Async.RunSync

        member cluster.NukeSqsQueues(?queueNameFilter : string -> bool) =
            cluster.NukeSqsQueuesAsync(?queueNameFilter = queueNameFilter) |> Async.RunSync

        member cluster.NukeAllResources(?nameFilter : string -> bool) =
            cluster.NukeS3Buckets(?bucketNameFilter = nameFilter)
            cluster.NukeDynamoDBTables(?tableNameFilter = nameFilter)
            cluster.NukeSqsQueues(?queueNameFilter = nameFilter)


type ClusterSession(config : MBrace.AWS.Configuration, localWorkerCount : int, ?heartbeatThreshold : TimeSpan) =

    static do AWSWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.awsworker.exe"
    
    let heartbeatThreshold = defaultArg heartbeatThreshold (TimeSpan.FromSeconds 10.)
    let lockObj = obj ()
    let mutable state = None

    let attachWorkers (cluster : AWSCluster) =
        cluster.AttachLocalWorkers(workerCount = localWorkerCount, logLevel = LogLevel.Debug, 
                    heartbeatThreshold = heartbeatThreshold, quiet = false, maxWorkItems = 16)

    member __.Start () =
        lock lockObj (fun () ->
            match state with
            | Some _ -> invalidOp "MBrace runtime already initialized."
            | None -> 
                let cluster = AWSCluster.Connect(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                if localWorkerCount > 0 then 
                    cluster.Reset(deleteUserData = true, deleteRuntimeState = true, deleteQueues = true, deleteLogs = true, force = true, reactivate = true)
                    attachWorkers cluster

                while cluster.Workers.Length < localWorkerCount do Thread.Sleep 100
                state <- Some cluster)

    member __.Stop () =
        lock lockObj (fun () ->
            match state with
            | None -> ()
            | Some r -> 
                if localWorkerCount > 0 then 
                    r.KillAllLocalWorkers()

                r.Reset(deleteUserData = true, deleteRuntimeState = true, deleteQueues = true, deleteLogs = true, force = true, reactivate = false)

                state <- None)

    member __.Cluster =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r

    member __.Chaos() =
        if localWorkerCount < 1 then () else
        lock lockObj (fun () ->
            let cluster = __.Cluster
            cluster.KillAllLocalWorkers()
            while cluster.Workers.Length > 0 do Thread.Sleep 500
            attachWorkers cluster
            while cluster.Workers.Length < localWorkerCount do Thread.Sleep 500)