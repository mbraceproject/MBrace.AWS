﻿#I "../../bin"
#r "FsPickler.dll"
#r "FsPickler.Json.dll"
#r "AWSSDK.Core.dll"
#r "AWSSDK.S3.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "AWSSDK.SQS.dll"
#r "Vagabond.dll"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.AWS.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2

open MBrace.Core
open MBrace.Core.Internals
open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Store

AWSWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.awsworker.exe"
let config = Configuration.FromCredentialsStore(AWSRegion.EUCentral1, resourcePrefix = "eirikmbrace")

let cluster = AWSCluster.InitOnCurrentMachine(config, workerCount = 3, logger = ConsoleLogger())
//let cluster = AWSCluster.Connect(config, logger = ConsoleLogger())
cluster.Reset(force = true)

cluster.Run(cloud { return 42 })

let proc = cluster.CreateProcess(cloud { return! Cloud.Parallel [for i in 1 .. 20 -> Cloud.CurrentWorker]})

proc.Status
cluster.Workers

cluster.ShowWorkers()
cluster.ClearSystemLogs()

cluster.ShowSystemLogs()

cluster.ShowProcesses()
cluster.ClearAllProcesses()

let worker = cluster.Workers.[0] :> IWorkerRef
let proc' = cluster.CreateProcess(Cloud.Sleep 10000, target = worker)
proc'.Result

cloud { let! w = Cloud.CurrentWorker in return Some w }
|> Cloud.ChoiceEverywhere
|> cluster.Run
//let workers = cluster.Run(Cloud.ChoiceEverywhere { let! w = Cloud.CurrentWorker )

let test () = cloud {
    let! counter = CloudAtom.New 0
    let worker i = cloud { 
        if i = 9 then
            invalidOp "failure"
        else
            do! Cloud.Sleep 15000
            do! CloudAtom.Increment counter |> Local.Ignore
    }

    try do! Array.init 20 worker |> Cloud.Parallel |> Cloud.Ignore
    with :? System.InvalidOperationException -> ()
    return counter
}

let c = cluster.Run(test())

c.Value

CloudAtom.Increment c |> cluster.RunLocally