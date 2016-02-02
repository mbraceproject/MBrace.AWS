namespace MBrace.AWS.Runtime

open System

open Nessos.FsPickler

open Amazon.DynamoDBv2
open Amazon.S3
open Amazon.SQS

open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.AWS
open MBrace.AWS.Runtime.Utilities

/// Serializable state/configuration record uniquely identifying an MBrace.AWS cluster
[<AutoSerializable(true); StructuralEquality; StructuralComparison>]
[<StructuredFormatDisplay("{Id}")>]
type ClusterId =
    {   
        /// Runtime version string
        Version : string

        S3Account       : AWSAccount
        DynamoDBAccount : AWSAccount
        SQSAccount      : AWSAccount
               
        WorkItemQueue   : string // SQS Name
        WorkItemTopic   : string // SNS Topic

        RuntimeS3Bucket     : string // Runtime S3 bucket name
        RuntimeTable        : string // Runtime DynamoDB table name
        RuntimeLogsTable    : string // Runtime logs DynamoDB table name

        UserDataS3Bucket    : string // User data bucket
        UserDataTable       : string // User data DynamoDB table name

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

    member private this.DeleteTable(tableName : string) = async {
        let! ct = Async.CancellationToken
        let! _ = this.DynamoDBAccount.DynamoDBClient.DeleteTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
        return ()
    }

    member private this.DeleteBucket(bucketName : string) = async {
        let! ct = Async.CancellationToken
        let! _ = this.S3Account.S3Client.DeleteBucketAsync(bucketName, ct) |> Async.AwaitTaskCorrect
        return ()
    }

    member this.ClearUserData() = async {
        do!
            [|
                this.DeleteTable this.UserDataTable
                this.DeleteBucket this.UserDataS3Bucket
            |]
            |> Async.Parallel
            |> Async.Ignore
    }


    member this.ClearRuntimeState() = async {
        do!
            [|
                this.DeleteTable this.RuntimeTable
                this.DeleteBucket this.RuntimeS3Bucket
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.ClearRuntimeLogs() = async {
        do! this.DeleteTable this.RuntimeLogsTable
    }

    member this.ClearRuntimeQueues() = async {
        let! ct = Async.CancellationToken
        do!
            [|
                this.SQSAccount.SQSClient.CreateQueueAsync(this.WorkItemQueue, ct) |> Async.AwaitTaskCorrect |> Async.Ignore
                this.SQSAccount.SNSClient.CreateTopicAsync(this.WorkItemTopic, ct) |> Async.AwaitTaskCorrect |> Async.Ignore
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///   Initializes all store resources on which the current runtime depends.  
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries on conflicts. Defaults to infinite retries.</param>
    /// <param name="retryInterval">Retry sleep interval. Defaults to 3000ms.</param>
    member this.InitializeAllStoreResources(?maxRetries : int, ?retryInterval : int) = async {
        let createTable name = this.DynamoDBAccount.DynamoDBClient.CreateTableIfNotExistsSafe(name, ?maxRetries = maxRetries, ?retryInterval = retryInterval)
        let createBucket name = this.S3Account.S3Client.CreateBucketIfNotExistsSafe(name, ?maxRetries = maxRetries, ?retryInterval = retryInterval)
        do!
            [|  
                createTable this.RuntimeTable
                createTable this.UserDataTable
                createTable this.RuntimeLogsTable

                createBucket this.RuntimeS3Bucket
                createBucket this.UserDataS3Bucket
            |]
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///     Activates a cluster id instance using provided configuration object.
    /// </summary>
    /// <param name="configuration">Azure cluster configuration object.</param>
    static member Activate(configuration : Configuration) =
        ProcessConfiguration.EnsureInitialized()
        let version = Version.Parse configuration.Version

        {
            Version             = version.ToString(4)

            S3Account           = AWSAccount.Create(configuration.S3Credentials.Credentials, configuration.S3Region.RegionEndpoint)
            DynamoDBAccount     = AWSAccount.Create(configuration.DynamoDBCredentials.Credentials, configuration.DynamoDBRegion.RegionEndpoint)
            SQSAccount          = AWSAccount.Create(configuration.SQSCredentials.Credentials, configuration.SQSRegion.RegionEndpoint)
               
            WorkItemQueue       = configuration.WorkItemQueue
            WorkItemTopic       = configuration.WorkItemTopic

            RuntimeS3Bucket     = configuration.RuntimeBucket
            RuntimeTable        = configuration.RuntimeTable
            RuntimeLogsTable    = configuration.RuntimeLogsTable

            UserDataS3Bucket    = configuration.UserDataBucket
            UserDataTable       = configuration.UserDataTable

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