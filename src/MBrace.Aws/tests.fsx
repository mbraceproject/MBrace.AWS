#r "../../packages/FsPickler/lib/net40/FsPickler.dll"
#r "../../packages/AWSSDK.Core/lib/net45/AWSSDK.Core.dll"
#r "../../packages/AWSSDK.S3/lib/net45/AWSSDK.S3.dll"
#r "../../packages/AWSSDK.DynamoDBv2/lib/net45/AWSSDK.DynamoDBv2.dll"
#r "../../packages/AWSSDK.SQS/lib/net45/AWSSDK.SQS.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3

open Nessos.FsPickler

let key = ""
let secret = ""

let client = new AmazonS3Client(key, secret, RegionEndpoint.EUCentral1) :> IAmazonS3

let bucks = client.ListBuckets().Buckets |> Seq.toArray

client.GetBucketLocation "mbracetest"

let buck2 = bucks.[1]

client.UploadObjectFromFilePath("mbracetest", "foo.png", "/Users/eirik/Desktop/untitled.png", new System.Collections.Generic.Dictionary<_,_>())
client.UploadObjectFromFilePath("mbracetest", "foo/bar/proedros.mp4", "/Users/eirik/Desktop/video-1450800103.mp4.mp4", new System.Collections.Generic.Dictionary<_,_>())

let listed = client.ListObjects("mbracetest").S3Objects |> Seq.toArray

client.PutBucket("mbracetest")

listed.[1]

client.U