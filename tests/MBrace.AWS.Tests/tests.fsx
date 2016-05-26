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

open System
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
let config = Configuration.FromCredentialsStore(AWSRegion.EUCentral1)

let cluster = AWSCluster.InitOnCurrentMachine(config, workerCount = 2, logger = ConsoleLogger(), heartbeatThreshold = TimeSpan.FromSeconds 20.)
//let cluster = AWSCluster.Connect(config, logger = ConsoleLogger())
//cluster.Reset(reactivate = false, force = true, deleteUserData = true)

cluster.ShowWorkers()

cluster.Run(Cloud.ParallelEverywhere Cloud.CurrentWorker)