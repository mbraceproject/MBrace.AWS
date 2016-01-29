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

let account = AWSAccount.Create("Default", RegionEndpoint.EUCentral1)
let store = S3FileStore.Create(account) :> ICloudFileStore

let run x = Async.RunSynchronously x

let clearBuckets() = async {
    let! buckets = store.EnumerateDirectories "/"
    let! _ = buckets |> Seq.filter (fun b -> b.StartsWith "/mbrace") |> Seq.map (fun b -> store.DeleteDirectory(b,true)) |> Async.Parallel
    return ()
}

clearBuckets() |> run

/////////////////////

open System

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model


let config = Configuration.FromCredentialsStore(AWSRegion.EUCentral1, "eirikmbrace")

config