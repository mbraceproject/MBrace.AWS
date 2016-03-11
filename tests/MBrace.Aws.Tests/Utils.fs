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
open MBrace.Runtime.Components
open MBrace.ThreadPool

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Store

#nowarn "445"

[<AutoOpen>]
module Utils =

    let init() = ProcessConfiguration.InitAsClient()

    let getEnvironmentVariable (envName:string) =
        let aux found target =
            if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
            else found

        Array.fold aux null [|EnvironmentVariableTarget.Process; EnvironmentVariableTarget.User; EnvironmentVariableTarget.Machine|]
        
    let getEnvironmentVariableOrDefault envName defaultValue = 
        match getEnvironmentVariable envName with
        | null | "" -> defaultValue
        | ev -> ev

    let getTestRegion () = 
        match getEnvironmentVariable "MBrace.AWS.Tests.Region" with
        | null | "" -> RegionEndpoint.EUCentral1
        | region -> RegionEndpoint.GetBySystemName region

    let getAWSProfile () = getEnvironmentVariableOrDefault "MBrace.AWS.Test.ProfileName" "default"
    let tryGetAWSCredentials () = 
        match getEnvironmentVariable "MBrace.AWS.Test.Credentials" with
        | null | "" -> None
        | creds -> 
            let toks = creds.Split(',')
            let creds = new BasicAWSCredentials(toks.[0], toks.[1]) :> Amazon.Runtime.AWSCredentials
            Some creds

    let getAWSTestAccount () =
        let region = getTestRegion()
        match tryGetAWSCredentials() with
        | Some creds -> AWSAccount.Create(creds, region)
        | None -> AWSAccount.Create(getAWSProfile(), region)

    let getMBraceConfig id =
        let rp = 
            match id with
            | None -> sprintf "tests%04d" <| Random().Next(0, 10000)
            | Some id -> id

        let acc = getAWSTestAccount()
        // TODO : fix API mess
        let region = AWSRegion.Parse acc.Region.SystemName
        let credentials = 
            let c = acc.Credentials.GetCredentials()
            { AccessKey = c.AccessKey ; SecretKey = c.SecretKey }

        new Configuration(AWSRegion.Parse acc.Region.SystemName, credentials, rp)
        


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