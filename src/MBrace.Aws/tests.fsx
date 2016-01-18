#I "../../bin"
#r "FsPickler.dll"
#r "AWSSDK.Core.dll"
#r "AWSSDK.S3.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "AWSSDK.SQS.dll"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Aws.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Aws.Runtime
open MBrace.Aws.Store

let account = AwsAccount.Create("Default", RegionEndpoint.EUCentral1)
let store = S3FileStore.Create(account) :> ICloudFileStore

let run x = Async.RunSynchronously x

let clearBuckets() = async {
    let! buckets = store.EnumerateDirectories "/"
    let! _ = buckets |> Seq.filter (fun b -> b.StartsWith "/mbrace") |> Seq.map (fun b -> store.DeleteDirectory(b,true)) |> Async.Parallel
    return ()
}

clearBuckets() |> run

let dir = store.GetRandomDirectoryName()
store.DirectoryExists dir |> run
store.CreateDirectory dir |> run
store.DirectoryExists dir |> run

store.EnumerateDirectories store.RootDirectory |> run

open Amazon.Runtime

let e = try store.BeginRead "/poutses/mple.txt" |> run ; failwith "" with e -> e :?> AmazonServiceException

open System.Net

e.StatusCode

StoreException.NotFound e