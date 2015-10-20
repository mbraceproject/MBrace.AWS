namespace MBrace.Aws.Runtime

open System.Runtime.Serialization

open Amazon
open Amazon.DynamoDBv2
open Amazon.S3
open Amazon.SQS

[<AutoSerializable(false); NoEquality; NoComparison>]
type private AwsS3AccountData = 
    { 
        AccessKey      : string
        AccessSecret   : string
        RegionEndpoint : RegionEndpoint
    }

[<Sealed; DataContract>]
type AwsS3Account private (?accountData : AwsS3AccountData) =
    let s3Client = 
        match accountData with
        | Some data 
            -> new AmazonS3Client(
                data.AccessKey, 
                data.AccessSecret, 
                data.RegionEndpoint)
        | _ -> new AmazonS3Client()

    member __.S3Client  = s3Client :> IAmazonS3