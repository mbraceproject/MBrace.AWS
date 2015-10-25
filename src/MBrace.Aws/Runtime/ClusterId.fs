namespace MBrace.Aws.Runtime

open MBrace.Runtime

/// Serializable state/configuration record uniquely identifying an MBrace.Azure cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {
        /// Runtime version string
        Version : string

        S3Account : AwsS3Account

        DynamoDBAccount : AwsDynamoDBAccount

        SQSAccount : AwsSQSAccount
    }
with
    member __.Id = 
        sprintf "{ S3 = \"%s\"; SQS = \"%s\"; DynamoDB = \"%s\" }" 
                "foo"   // TODO
                "bar"   // TODO
                "zoo"   // TODO

    interface IRuntimeId with 
        member this.Id = this.Id