namespace MBrace.Aws.Runtime

open System.Runtime.Serialization

open Amazon

[<AutoSerializable(false); NoEquality; NoComparison>]
type private AwsAccountData = 
    { 
        AccessKey      : string
        AccessSecret   : string
        RegionEndpoint : RegionEndpoint
    }

open Amazon.S3

[<Sealed; DataContract>]
type AwsS3Account private (?accountData : AwsAccountData) =
    let s3Client = 
        match accountData with
        | Some data 
            -> new AmazonS3Client(
                data.AccessKey, 
                data.AccessSecret, 
                data.RegionEndpoint)
        | _ -> new AmazonS3Client()

    member __.S3Client  = s3Client :> IAmazonS3

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel

[<Sealed; DataContract>]
type AwsDynamoDBAccount private (?accountData : AwsAccountData) =
    let dynamoDBClient = 
        match accountData with
        | Some data 
            -> new AmazonDynamoDBClient(
                data.AccessKey, 
                data.AccessSecret, 
                data.RegionEndpoint)
        | _ -> new AmazonDynamoDBClient()

    member __.DynamoDBClient  = dynamoDBClient :> IAmazonDynamoDB
    member __.DynamoDBContext = new DynamoDBContext(dynamoDBClient)