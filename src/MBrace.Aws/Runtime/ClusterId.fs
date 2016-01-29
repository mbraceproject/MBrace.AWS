namespace MBrace.AWS.Runtime

open Nessos.FsPickler
open MBrace.Runtime

open MBrace.AWS

/// Serializable state/configuration record uniquely identifying an MBrace.AWS cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {   
        Region : AWSRegion

        /// Runtime version string
        Version : string

        S3Account       : AWSAccount
        DynamoDBAccount : AWSAccount
        SQSAccount      : AWSAccount
               
        WorkItemQueue   : string // SQS Name
        WorkItemTopic   : string // SNS Topic

        RuntimeS3Bucket      : string // Runtime S3 bucket name
        RuntimeTable         : string // Runtime DynamoDB table name
        RuntimeLogsTable     : string // Runtime logs DynamoDB table name
        RuntimeUserDataTable : string // User data DynamoDB table name

        /// Specifies whether closure serialization
        /// should be optimized using closure sifting.
        OptimizeClosureSerialization : bool
    }

    member this.Id = 
        let hash = FsPickler.ComputeHash this
        let enc = System.Convert.ToBase64String hash.Hash
        sprintf "AWS runtime @ %s hashId:%s" this.Region.SystemName enc

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