namespace MBrace.AWS.Runtime

open System
open System.Text.RegularExpressions

[<RequireQualifiedAccess>]
module Validate =

    let inline private validate (r : Regex) (input : string) = r.IsMatch input

    let private hostNameRegex = Regex("^[a-z0-9]+$", RegexOptions.Compiled)
    let hostname (hostname : string) =
        if not <| validate hostNameRegex hostname then 
            sprintf "'%s' is not a valid DNS hostname." hostname
            |> invalidArg "hostname"

    // S3 Bucket name restrictions, see:
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    let private bucketNameRegex = Regex("^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$", RegexOptions.Compiled)
    let tryBucketName (bucketName : string) =
        if bucketName = null || bucketName.Length < 3 || bucketName.Length > 63 then false
        elif not <| validate bucketNameRegex bucketName then false
        else true

    let bucketName (bucketName : string) =
        if not <| tryBucketName bucketName then
            sprintf "Invalid S3 bucket name '%s', see %s" 
                bucketName "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
            |> invalidArg "bucketName" 

    // S3 Bucket name restrictions, see:
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    let private recommendedKeyNameRegex = Regex("^[\w!\-_\.\*'\(\)/]*$", RegexOptions.Compiled)
    let tryKeyName forceRecommendedChars (keyName : string) =
        if System.String.IsNullOrEmpty keyName then false
        elif System.Text.Encoding.UTF8.GetByteCount keyName > 1024 then false
        elif forceRecommendedChars && not <| validate recommendedKeyNameRegex keyName then false
        else true

    let keyName forceRecommendedChars keyName =
        if not <| tryKeyName forceRecommendedChars keyName then
            sprintf "Invalid S3 key name '%s', see %s" 
                    keyName "http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html"
            |> invalidArg "keyName"
        

    // DynamoDB Name limitations, see:
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
    let private tableNameRegex = Regex("^[\w\-_\.]*$", RegexOptions.Compiled)
    let tryTableName (tableName : string) =
        if tableName.Length < 3 || tableName.Length > 255 then false
        elif not <| validate tableNameRegex tableName then false
        else true

    let tableName tableName =
        if not <| tryTableName tableName then
            sprintf "Invalid DynamoDB table name '%s', see %s" 
                    tableName "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html"
            |> invalidArg "tableName"


    // SQS Name limitations, see: https://aws.amazon.com/sqs/faqs/
    let private queueNameRegex = Regex("^[a-zA-Z0-9\-_]+$", RegexOptions.Compiled)
    let tryQueueName (queueName : string) =
        if queueName.Length > 80 then false
        elif not <| validate queueNameRegex queueName then false
        else true

    let queueName queueName =
        if not <| tryQueueName queueName then
            sprintf "Invalid SQS queue name '%s', see %s" 
                    queueName "https://aws.amazon.com/sqs/faqs/"
            |> invalidArg "queueName"