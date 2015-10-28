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

        /// Specifies whether closure serialization
        /// should be optimized using closure sifting.
        OptimizeClosureSerialization : bool
    }
with
    member __.Id = 
        sprintf "{ S3 = \"%s\"; SQS = \"%s\"; DynamoDB = \"%s\" }" 
                "foo"   // TODO
                "bar"   // TODO
                "zoo"   // TODO

    interface IRuntimeId with 
        member this.Id = this.Id

open System
open System.Collections.Concurrent

/// Dependency injection facility for Specific cluster instances
[<Sealed; AbstractClass>]
type ConfigurationRegistry private () =
    static let registry = new ConcurrentDictionary<ClusterId * Type, obj>()

    static member Register<'T>(clusterId : ClusterId, item : 'T) : unit =
        ignore <| registry.TryAdd((clusterId, typeof<'T>), item :> obj)

    static member Resolve<'T>(clusterId : ClusterId) : 'T =
        let mutable result = null
        if registry.TryGetValue((clusterId, typeof<'T>), &result) then result :?> 'T
        else
            invalidOp <| sprintf "Could not resolve Resource of type %A for ConfigurationId %A" clusterId typeof<'T>