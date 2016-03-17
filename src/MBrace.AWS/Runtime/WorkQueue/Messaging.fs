namespace MBrace.AWS.Runtime

open System
open System.IO
open System.Threading.Tasks

open FSharp.AWS.DynamoDB

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS.Store

type internal MessagingClient =

    static member TryDequeue 
            (clusterId     : ClusterId, 
             logger        : ISystemLogger, 
             localWorkerId : IWorkerId, 
             dequeue       : unit -> Async<SqsDequeueMessage option>) 
            : Async<ICloudWorkItemLeaseToken option> = async {

        // quickly discard messages that have already been canceled
        let rec dequeueCanceled() = async {
            let! res = dequeue()
            match res with
            | None -> return None
            | Some msg ->
                let workInfo = WorkItemMessage.FromDequeuedSqsMessage msg
                if workInfo.GetCancellationToken(clusterId).IsCancellationRequested then
                    logger.Logf LogLevel.Debug "%O : discarding canceled work item." workInfo
                    let! _ = Async.StartChild(msg.Complete())
                    return! dequeueCanceled()
                else
                    return Some(workInfo, msg)
        }

        let! res = dequeueCanceled()
        match res with
        | None -> return None
        | Some (workInfo, msg) ->
            logger.Logf LogLevel.Debug "%O : dequeued" workInfo
            logger.Logf LogLevel.Debug "%O : starting lock renew loop" workInfo
            let monitor = WorkItemLeaseMonitor.Start(msg, workInfo, logger)

            let table = clusterId.GetRuntimeTable<WorkItemRecord>()
            try
                let! oldRecord = table.GetItemAsync(workInfo.TableKey)
                let faultCount = oldRecord.DeliveryCount

                // determine the fault info for the dequeued work item
                let faultInfo =
                    match workInfo.TargetWorker with
                    | Some target when target <> localWorkerId.Id -> 
                        // a targeted work item that has been dequeued by a different worker is to be declared faulted
                        if faultCount > 0 then 
                            WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(target))
                        else
                            IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))

                    | _ when faultCount = 0 -> NoFault
                    | _ ->
                        match oldRecord.FaultInfo with
                        | FaultInfo.FaultDeclaredByWorker -> // a fault exception has been set by the executing worker
                            let lastExn = oldRecord.LastException |> Option.get
                            let lastWorker = new WorkerId(oldRecord.CurrentWorker |> Option.get)
                            FaultDeclaredByWorker(faultCount, lastExn, lastWorker)
                        | _ -> 
                            // a worker has died while previously dequeueing the worker
                            // account for cases where worker died before even updating the work item record
                            let previousWorker = defaultArg oldRecord.CurrentWorker "<unknown>"
                            WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(previousWorker))

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" workInfo faultInfo
                logger.Logf LogLevel.Debug "%O : changing status to %A" workInfo WorkItemStatus.Dequeued

                let updateOp = setWorkItemDequeued localWorkerId.Id DateTimeOffset.Now (faultInfo.ToEnum())

                let! _ = table.UpdateItemAsync(workInfo.TableKey, updateOp)

                logger.Logf LogLevel.Debug "%O : changed status successfully" workInfo
                let! leaseToken = WorkItemLeaseToken.Create(clusterId, workInfo, monitor, faultInfo)
                return Some (leaseToken :> ICloudWorkItemLeaseToken)

            with e ->
                monitor.CompleteWith Abandon // in case of dequeue exception, abandon lease renew loop
                return! Async.Raise e
    }

    static member Enqueue 
            (clusterId     : ClusterId, 
             logger        : ISystemLogger, 
             workItem      : CloudWorkItem, 
             allowNewSifts : bool, 
             send          : WorkItemMessage -> Async<unit>) = async { 

        // Step 1: Persist work item payload to S3
        let blobUri = sprintf "workItem/%s/%s" workItem.Process.Id (fromGuid workItem.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Single workItem, blobUri, allowNewSifts)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 2: create record entry in DynamoDB
        let record = WorkItemRecord.FromCloudWorkItem(workItem, size)
        let! _ = clusterId.GetRuntimeTable<WorkItemRecord>().PutItemAsync(record, itemDoesNotExist)

        // Step 3: send work item message to SQS
        let msg : WorkItemMessage = 
            { 
                Version      = ProcessConfiguration.Version
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker |> Option.bind (fun x -> Some x.Id)
                CancellationToken = workItem.CancellationToken |> DynamoDBCancellationToken.ToUUID
                BatchIndex   = None
            }

        do! send msg

        logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize size)
    }

    static member EnqueueBatch
            (clusterId : ClusterId, 
             logger    : ISystemLogger, 
             jobs      : CloudWorkItem[], 
             send      : seq<WorkItemMessage> -> Async<unit>) = async { 
        // silent discard if empty
        if jobs.Length = 0 then return () else

        // Step 1: persist payload to S3
        let headJob = jobs.[0]
        let blobUri = sprintf "workItem/%s/batch/%s" headJob.Process.Id (fromGuid headJob.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Batch jobs, blobUri, allowNewSifts = false)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 2: populate work item records
        let table = clusterId.GetRuntimeTable<WorkItemRecord>()
        let workItems = jobs |> Seq.map (fun j -> WorkItemRecord.FromCloudWorkItem(j, size))
        let! _ = 
            workItems
            |> Seq.chunksOf 25
            |> Seq.map table.BatchPutItemsAsync
            |> Async.Parallel

        // Step 3: create work messages and post to S3
        let mkWorkItemMessage (i : int) (workItem : CloudWorkItem) : WorkItemMessage =
            {
                Version      = ProcessConfiguration.Version
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker |> Option.bind (fun x -> Some x.Id)
                CancellationToken = workItem.CancellationToken |> DynamoDBCancellationToken.ToUUID
                BatchIndex   = Some i
            }

        let messages = jobs |> Array.mapi mkWorkItemMessage
        do! send messages
        logger.LogInfof 
            "Enqueued batched jobs of %d items for task %s, total size %s." 
            jobs.Length 
            headJob.Process.Id 
            (getHumanReadableByteSize size)
    }

/// Queue client implementation
[<Sealed; AutoSerializable(false)>]
type internal Queue private (clusterId : ClusterId, queueUri : string, logger : ISystemLogger) = 
    let client = clusterId.SQSAccount.SQSClient
    
    let tryDequeue () = client.TryDequeue(queueUri, 
                                            visibilityTimeout = WorkItemQueueSettings.VisibilityTimeout,
                                            timeoutMilliseconds = WorkItemQueueSettings.DequeueTimeout)

    let send (msg:WorkItemMessage) =
        let body = msg.ToSqsMessageBody()
        client.Enqueue(queueUri, body)

    let sendBatch (msgs:seq<WorkItemMessage>) =
        let bodies = msgs |> Seq.map (fun m -> m.ToSqsMessageBody(), None)
        client.EnqueueBatch(queueUri, bodies)

    member __.GetMessageCountAsync() = client.GetMessageCount(queueUri)

    member __.EnqueueBatch(jobs : CloudWorkItem []) = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, sendBatch)
            
    member __.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, send)
    
    member __.TryDequeue(workerId : IWorkerId) = 
        MessagingClient.TryDequeue(clusterId, logger, workerId, tryDequeue)

    member __.EnqueueMessagesBatch(messages : seq<WorkItemMessage>) = async {
        do! sendBatch messages
    }
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = async { 
        let account = clusterId.SQSAccount.SQSClient
        let! exists, queueUri = account.EnsureQueueNameExists clusterId.WorkItemQueueName
        if exists then
            logger.LogInfof "Queue %A already exists." clusterId.WorkItemQueueName
        else
            logger.LogInfof "Creating new SQS queue %A" clusterId.WorkItemQueueName

        return new Queue(clusterId, queueUri, logger)
    }

/// Topic subscription client implemented as a worker specific SQS queue
[<Sealed; AutoSerializable(false)>]
type internal Subscription internal (clusterId : ClusterId, workerId : string, logger : ISystemLogger) =
    static let getQueueName topic workerId = topic + "-" + workerId

    let client    = clusterId.SQSAccount.SQSClient
    let topic     = clusterId.WorkItemTopicName
    let queueName = getQueueName topic workerId
    let mutable queueUri = None

    let tryGetQueueUri() = async {
        match queueUri with
        | Some _ -> return queueUri
        | None ->
            let! uri = client.TryGetQueueUri queueName
            if Option.isSome uri then queueUri <- uri
            return uri
    }

    let getQueueUriOrCreate () = async {
        match queueUri with
        | Some uri -> return uri
        | None ->
            let! uri = client.CreateQueueWithName queueName
            queueUri <- Some uri
            return uri
    }

    member __.WorkerId = workerId

    member __.GetMessageCountAsync() = async {
        let! qUri = tryGetQueueUri()
        match qUri with
        | None -> return 0
        | Some uri -> 
            try return! client.GetMessageCount uri
            with QueueNotFoundException -> return 0
    }

    member __.TryDequeue(currentWorker : IWorkerId) = async {
        let tryDequeue() = async {
            let! qUri = tryGetQueueUri()
            match qUri with
            | None -> return None
            | Some uri -> 
                try 
                    return! client.TryDequeue(uri,
                                visibilityTimeout = WorkItemQueueSettings.VisibilityTimeout,
                                timeoutMilliseconds = WorkItemQueueSettings.DequeueTimeout)
                with QueueNotFoundException -> return None
        }
        
        return! MessagingClient.TryDequeue(clusterId, logger, currentWorker, tryDequeue)
    }

    member __.Enqueue(msg : WorkItemMessage) = async {
        try
            let! queueUri = getQueueUriOrCreate()
            do! client.Enqueue(queueUri, msg.ToSqsMessageBody())
        with QueueNotFoundException ->
            queueUri <- None
            do! __.Enqueue(msg)
    }

    member __.EnqueueBatch(msgs : seq<WorkItemMessage>) = async {
        let bodies = msgs |> Seq.map (fun m -> m.ToSqsMessageBody(), None) |> Seq.toArray
        let rec aux () = async {
            try
                let! queueUri = getQueueUriOrCreate()
                do! client.EnqueueBatch(queueUri, bodies)
            with QueueNotFoundException ->
                queueUri <- None
                do! aux ()
        }

        do! aux ()
    }

    member __.DequeueAllMessagesBatch() = async {
        let! uri = tryGetQueueUri()
        match uri with
        | Some queueUri -> 
            try return! client.DequeueAll(queueUri, visibilityTimeout = WorkItemQueueSettings.VisibilityTimeout)
            with QueueNotFoundException -> return [||]
        | _ -> return [||]
    }

    member __.Delete(messages : seq<SqsDequeueMessage>) = client.DeleteBatch(messages)

/// Topic client implementation as a worker specific SQS queue
[<Sealed; AutoSerializable(false)>]
type internal Topic private (clusterId : ClusterId, logger : ISystemLogger) = 

    let getSubscription = concurrentMemoize (fun id -> new Subscription(clusterId, id, logger))

    let enqueue (msg : WorkItemMessage) = async {
        let subscription = getSubscription (Option.get msg.TargetWorker)
        do! subscription.Enqueue(msg)
    }

    let enqueueBatch (msgs : seq<WorkItemMessage>) = async {
        do!
            msgs
            |> Seq.groupBy (fun m -> Option.get m.TargetWorker)
            |> Seq.map (fun (id, msgs) -> async {
                let subscription = getSubscription id
                do! subscription.EnqueueBatch(msgs) })
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.GetSubscription(subscriptionId : IWorkerId) = 
        getSubscription subscriptionId.Id
        
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, enqueueBatch)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, enqueue)

    static member Create(clusterId : ClusterId, logger : ISystemLogger) = 
        new Topic(clusterId, logger)