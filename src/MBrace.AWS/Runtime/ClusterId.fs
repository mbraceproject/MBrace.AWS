namespace MBrace.AWS.Runtime

open System

open Nessos.FsPickler

open Amazon.DynamoDBv2
open Amazon.S3
open Amazon.SQS

open FSharp.AWS.DynamoDB

open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.AWS
open MBrace.AWS.Runtime.Utilities

[<AutoOpen>]
module private TableKeySchema =

    type DefaultKeySchema =
        {
            [<HashKey>] HashKey : string
            [<RangeKey>] RangeKey : string
        }

    let keySchema = RecordTemplate.Define<DefaultKeySchema>().PrimaryKey

    let verify (ctx : TableContext<'T>) =
        if ctx.PrimaryKey <> keySchema then
            invalidArg (string typeof<'T>) "invalid key schema"
        ctx

/// Serializable state/configuration record uniquely identifying an MBrace.AWS cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {   
        /// Runtime version string
        ResourcePrefix : string

        S3Account       : AWSAccount
        DynamoDBAccount : AWSAccount
        SQSAccount      : AWSAccount
               
        WorkItemQueueName     : string // SQS Queue
        WorkItemTopicName     : string // SQS Topic

        RuntimeS3BucketName   : string // Runtime S3 bucket name
        RuntimeTableName      : string // Runtime DynamoDB table name
        RuntimeLogsTableName  : string // Runtime logs DynamoDB table name

        UserDataS3BucketName  : string // User data bucket
        UserDataTableName     : string // User data DynamoDB table name

        /// Specifies whether closure serialization
        /// should be optimized using closure sifting.
        OptimizeClosureSerialization : bool
    }

with
    member this.Hash = 
        let hash = FsPickler.ComputeHash this
        System.Convert.ToBase64String hash.Hash


    member this.Id = 
        sprintf "AWS runtime [hashId:%s]" this.Hash

    interface IRuntimeId with 
        member this.Id = this.Id

    member __.GetRuntimeTable<'TSchema>() = 
        __.DynamoDBAccount.GetTableContext<'TSchema>(__.RuntimeTableName)
        |> verify

    member __.GetRuntimeLogsTable<'TSchema>() =
        __.DynamoDBAccount.GetTableContext<'TSchema>(__.RuntimeLogsTableName)
        |> verify

    member __.GetUserDataTable<'TSchema>() =
        __.DynamoDBAccount.GetTableContext<'TSchema>(__.UserDataTableName)
        |> verify

    member private this.DeleteTable(tableName : string) = async {
        do! this.DynamoDBAccount.DynamoDBClient.DeleteTableAsyncSafe(tableName)
    }

    member private this.DeleteBucket(bucketName : string) = async {
        do! this.S3Account.S3Client.DeleteBucketAsyncSafe bucketName
    }

    member this.ClearUserData() = async {
        do!
            [|
                this.DeleteTable this.UserDataTableName
                this.DeleteBucket this.UserDataS3BucketName
            |]
            |> Async.Parallel
            |> Async.Ignore
    }


    member this.ClearRuntimeState() = async {
        do!
            [|
                this.DeleteTable this.RuntimeTableName
                this.DeleteBucket this.RuntimeS3BucketName
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.ClearRuntimeLogs() = async {
        do! this.DeleteTable this.RuntimeLogsTableName
    }

    member this.ClearRuntimeQueues() = async {
        let client = this.SQSAccount.SQSClient

        let deleteWorkItemQueue() = async {
            let! uri = client.TryGetQueueUri this.WorkItemQueueName
            match uri with
            | None -> ()
            | Some u -> do! client.DeleteQueueUri(u)
        }

        let deleteTopics () = async {
            let! topicUris = client.GetQueueUris(prefix = this.WorkItemTopicName)
            do!
                topicUris
                |> Seq.map client.DeleteQueueUri
                |> Async.Parallel
                |> Async.Ignore
        }

        let! dqt = deleteWorkItemQueue() |> Async.StartChild
        do! deleteTopics()
        let! _ = dqt
        return ()
    }

    /// <summary>
    ///   Initializes all store resources on which the current runtime depends.  
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries on conflicts. Defaults to infinite retries.</param>
    /// <param name="retryInterval">Retry sleep interval. Defaults to 3000ms.</param>
    member this.InitializeAllStoreResources(?maxRetries : int, ?retryInterval : int) = async {
        let createBucket name = this.S3Account.S3Client.CreateBucketIfNotExistsSafe(name, ?maxRetries = maxRetries, ?retryInterval = retryInterval)
        let throughput = new ProvisionedThroughput(25L, 25L)
        do!
            [|  
                this.GetRuntimeTable<DefaultKeySchema>().VerifyTableAsync(createIfNotExists = true, provisionedThroughput = throughput)
                this.GetUserDataTable<DefaultKeySchema>().VerifyTableAsync(createIfNotExists = true, provisionedThroughput = throughput)
                this.GetRuntimeLogsTable<DefaultKeySchema>().VerifyTableAsync(createIfNotExists = true, provisionedThroughput = throughput)

                createBucket this.RuntimeS3BucketName
                createBucket this.UserDataS3BucketName
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///     Activates a cluster id instance using provided configuration object.
    /// </summary>
    /// <param name="configuration">AWS cluster configuration object.</param>
    static member Activate(configuration : Configuration) =
        ProcessConfiguration.EnsureInitialized()
        {
            ResourcePrefix       = configuration.ResourcePrefix
                                 
            S3Account            = AWSAccount.Create(configuration.S3Credentials.Credentials, configuration.S3Region.RegionEndpoint)
            DynamoDBAccount      = AWSAccount.Create(configuration.DynamoDBCredentials.Credentials, configuration.DynamoDBRegion.RegionEndpoint)
            SQSAccount           = AWSAccount.Create(configuration.SQSCredentials.Credentials, configuration.SQSRegion.RegionEndpoint)
                                 
            WorkItemQueueName    = configuration.WorkItemQueue
            WorkItemTopicName    = configuration.WorkItemTopic
                                 
            RuntimeS3BucketName  = configuration.RuntimeBucket
            RuntimeTableName     = configuration.RuntimeTable
            RuntimeLogsTableName = configuration.RuntimeLogsTable
                                 
            UserDataS3BucketName = configuration.UserDataBucket
            UserDataTableName    = configuration.UserDataTable

            OptimizeClosureSerialization = configuration.OptimizeClosureSerialization
        }

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