namespace MBrace.AWS.Runtime

open System
open System.IO
open System.Threading.Tasks

open FSharp.DynamoDB

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
        let! res = dequeue()
        match res with
        | None -> return None
        | Some msg ->
            let workInfo = WorkItemMessage.FromReceivedMessage(msg)
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

        // Step 1: Persist work item payload to blob store
        let blobUri = sprintf "workItem/%s/%s" workItem.Process.Id (fromGuid workItem.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Single workItem, blobUri, allowNewSifts)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 2: create record entry in table store
        let record = WorkItemRecord.FromCloudWorkItem(workItem, size)
        let! _ = clusterId.GetRuntimeTable<WorkItemRecord>().PutItemAsync(record, itemDoesNotExist)

        // Step 3: send work item message to service bus queue
        let msg : WorkItemMessage = 
            { 
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker |> Option.bind (fun x -> Some x.Id)
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

        // Step 1: persist payload to blob store
        let headJob = jobs.[0]
        let blobUri = sprintf "workItem/%s/batch/%s" headJob.Process.Id (fromGuid headJob.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Batch jobs, blobUri, allowNewSifts = false)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 2: populate work item records
        let workItems = jobs |> Seq.map (fun j -> WorkItemRecord.FromCloudWorkItem(j, size))
        let! _ = clusterId.GetRuntimeTable<WorkItemRecord>().BatchPutItemsAsync(workItems)

        // Step 3: create work messages and post to service bus queue
        let mkWorkItemMessage (i : int) (workItem : CloudWorkItem) : WorkItemMessage =
            {
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker |> Option.bind (fun x -> Some x.Id)
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
type internal Queue (clusterId : ClusterId, queueUri, logger : ISystemLogger) = 
    let account = clusterId.SQSAccount
    let queue   = SQSCloudQueue<WorkItemMessage>(queueUri, account) :> CloudQueue<WorkItemMessage>

    let send msg = queue.EnqueueAsync msg
    let sendBatch msgs = queue.EnqueueBatchAsync msgs
    
    let tryDequeue () = Sqs.TryDequeue(account, queueUri)

    member __.GetMessageCountAsync() = Sqs.GetMessageCount(account, queueUri)

    member __.EnqueueBatch(jobs : CloudWorkItem []) = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, sendBatch)
            
    member __.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, send)
    
    member __.TryDequeue(workerId : IWorkerId) = 
        MessagingClient.TryDequeue(clusterId, logger, workerId, tryDequeue)

    member __.EnqueueMessagesBatch(messages : seq<WorkItemMessage>) = async {
        do! queue.EnqueueBatchAsync(messages)
    }
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = async { 
        let account   = clusterId.SQSAccount
        let! queueUri = Sqs.TryGetQueueUri(account, clusterId.WorkItemQueueName)
        match queueUri with
        | Some queueUri ->
            logger.LogInfof "Queue %A already exists." clusterId.WorkItemQueueName
            return new Queue(clusterId, queueUri, logger)
        | None ->
            logger.LogInfof "Creating new ServiceBus queue %A" clusterId.WorkItemQueueName
            // TODO : what should be the default queue attributes?
            let! queueUri = Sqs.CreateQueue(account, clusterId.WorkItemQueueName)
            return new Queue(clusterId, queueUri, logger)
    }

[<AutoOpen>]
module internal TopicUtils =
    let getQueueName topic workerId = topic + "-" + workerId

/// Topic subscription client implemented as a worker specific SQS queue
[<Sealed; AutoSerializable(false)>]
type internal Subscription 
        (clusterId      : ClusterId, 
         targetWorkerId : IWorkerId, 
         logger         : ISystemLogger) = 
    let account   = clusterId.SQSAccount
    let topic     = clusterId.WorkItemTopicName
    let workerId  = targetWorkerId.Id
    let queueName = getQueueName topic workerId

    // NOTE: the worker specific queue is created on push. 
    // This is to avoid creating new queues unnecessarily,
    // hence the need for tryGetQueueUri
    let tryDequeue () = async {
        let! qUri = Sqs.TryGetQueueUri(account, queueName)
        match qUri with
        | None -> return None
        | Some uri -> return! Sqs.TryDequeue(account, uri)
    }

    member __.TargetWorkerId = targetWorkerId

    member __.GetMessageCountAsync() = async {
        let! qUri = Sqs.TryGetQueueUri(account, queueName)
        match qUri with
        | None -> return 0
        | Some uri -> return! Sqs.GetMessageCount(account, uri)
    }

    member __.TryDequeue(currentWorker : IWorkerId) = 
        MessagingClient.TryDequeue(clusterId, logger, currentWorker, tryDequeue)

    member __.DequeueAllMessagesBatch() = async {
        let! uri = Sqs.TryGetQueueUri(account, queueName)
        match uri with
        | Some queueUri -> return! Sqs.DequeueAll(account, queueUri)
        | _ -> return [||]
    }

    member __.Delete(messages : seq<SqsDequeueMessage>) = Sqs.DeleteBatch(clusterId.SQSAccount, messages)

/// Topic client implementation as a worker specific SQS queue
[<Sealed; AutoSerializable(false)>]
type internal Topic (clusterId : ClusterId, logger : ISystemLogger) = 
    let account = clusterId.SQSAccount
    let topic   = clusterId.WorkItemTopicName
    
    let getQueueUriOrCreate (msg : WorkItemMessage) = async {
        // since this internal class is only called by the WorkItemQueue
        // we can safely assume that TargetWorker is Some in this case
        let workerId = Option.get msg.TargetWorker
        let queueName = getQueueName topic workerId
        let! queueUri = Sqs.TryGetQueueUri(account, queueName)
        match queueUri with
        | Some uri -> return uri
        | _ -> return! Sqs.CreateQueue(account, queueName)
    }

    let enqueue (msg : WorkItemMessage) = async {
        let! queueUri = getQueueUriOrCreate msg
        let body = msg.ToMessageAttributes()
        do! Sqs.Enqueue(account, queueUri, body)
    }

    let enqueueBatch (msgs : seq<WorkItemMessage>) = async {
        do!
            msgs
            |> Seq.groupBy (fun m -> Option.get m.TargetWorker)
            |> Seq.map (fun (_, msgs) -> async {
                let! queueUri = getQueueUriOrCreate (Seq.head msgs)
                let bodies = msgs |> Seq.map (fun m -> let b = m.ToMessageAttributes() in b, None)
                do! Sqs.EnqueueBatch(account, queueUri, bodies) })
            |> Async.Parallel
            |> Async.Ignore
    }

    member this.GetSubscription(subscriptionId : IWorkerId) = 
        new Subscription(clusterId, subscriptionId, logger)
        
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, enqueueBatch)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, enqueue)

    member __.Delete(workerId : IWorkerId) = async {
        let queueName = getQueueName topic workerId.Id
        let! queueUri = Sqs.TryGetQueueUri(account, queueName)
        match queueUri with
        | Some queueUri -> do! Sqs.DeleteQueue(account, queueUri)
        | None -> return ()
    }

    static member Create(clusterId, logger : ISystemLogger) = 
        new Topic(clusterId, logger)