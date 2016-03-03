#I "../../bin"
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

AWSWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/MBrace.AWS.StandaloneWorker.exe"
let config = Configuration.FromCredentialsStore(AWSRegion.EUCentral1, "eirikmbrace")

let cluster = AWSCluster.InitOnCurrentMachine(config, workerCount = 3)
//let cluster = AWSCluster.Connect(config)
cluster.Reset(force = true)

cluster.Run(Cloud.ParallelEverywhere Cloud.CurrentWorker)

let c = cluster.CreateCancellationTokenSource([c.Token])

c.Token.IsCancellationRequested

c.Cancel()


cluster.Workers

cluster.ShowWorkers()

cluster.ShowSystemLogs()