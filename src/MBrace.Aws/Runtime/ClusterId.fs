namespace MBrace.Aws.Runtime

open MBrace.Runtime

/// Serializable state/configuration record uniquely identifying an MBrace.Azure cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {
        /// Runtime version string
        Version : string

        S3Account       : AwsS3Account
        DynamoDBAccount : AwsDynamoDBAccount
        SQSAccount      : AwsSQSAccount

        /// SQS Name
        WorkItemQueue : string
        /// SNS Topic
        WorkItemTopic : string

        /// Runtime S3 bucket name
        RuntimeS3Bucket : string

        /// Runtime DynamoDB table name
        RuntimeTable : string
        /// Runtime logs DynamoDB table name
        RuntimeLogsTable : string
        /// User data DynamoDB table name
        RuntimeUserDataTable : string
    }
with
    member __.Id = 
        sprintf "{ S3 = \"%s\"; SQS = \"%s\"; DynamoDB = \"%s\" }" 
                "foo"   // TODO
                "bar"   // TODO
                "zoo"   // TODO

    interface IRuntimeId with 
        member this.Id = this.Id