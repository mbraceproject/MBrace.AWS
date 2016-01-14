#I "../../bin"
#r "FsPickler.dll"
#r "AWSSDK.Core.dll"
#r "AWSSDK.S3.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "AWSSDK.SQS.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Aws.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2

open Nessos.FsPickler
open MBrace.Aws.Runtime

let clone x = FsPickler.Clone x


let account = AwsAccount.Create("eirik-nessos", RegionEndpoint.EUCentral1)

let buckets = account.S3Client.ListBuckets().Buckets |> Seq.toArray

let buck = buckets.[0]

buck.BucketName

let account' = clone account

obj.ReferenceEquals(account.S3Client, account'.S3Client)