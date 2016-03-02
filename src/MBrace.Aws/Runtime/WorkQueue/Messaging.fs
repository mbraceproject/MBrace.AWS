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
             dequeue       : unit -> Task<(WorkItemMessage * WorkItemMessageAttributes) option>) 
            : Async<ICloudWorkItemLeaseToken option> = async { 
        let! res = dequeue() |> Async.AwaitTaskCorrect
        match res with
        | None -> return None
        | Some (message, attributes) ->
            let workInfo = WorkItemLeaseTokenInfo.FromReceivedMessage(message, attributes)
            logger.Logf LogLevel.Debug "%O : dequeued, delivery count = %d" workInfo workInfo.DeliveryCount 

            logger.Logf LogLevel.Debug "%O : starting lock renew loop" workInfo
            let monitor = WorkItemLeaseMonitor.Start(clusterId, workInfo, logger)

            let table = clusterId.GetRuntimeTable<WorkItemRecord>()

            try
                // determine the fault info for the dequeued work item
                let! faultInfo = async {
                    let faultCount = workInfo.DeliveryCount - 1
                    match workInfo.TargetWorker with
                    | Some target when target <> localWorkerId.Id -> 
                        // a targeted work item that has been dequeued by a different worker is to be declared faulted
                        return IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))

                    | _ when faultCount = 0 -> return NoFault
                    | _ ->
                        let! oldRecord = table.GetItemAsync(workInfo.TableKey)

                        match oldRecord.FaultInfo with
                        | FaultInfo.FaultDeclaredByWorker -> // a fault exception has been set by the executing worker
                            let lastExn = oldRecord.LastException |> Option.get
                            let lastWorker = new WorkerId(oldRecord.CurrentWorker |> Option.get)
                            return FaultDeclaredByWorker(faultCount, lastExn, lastWorker)
                        | _ -> 
                            // a worker has died while previously dequeueing the worker
                            // account for cases where worker died before even updating the work item record
                            let previousWorker = defaultArg oldRecord.CurrentWorker "<unknown>"
                            return WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(previousWorker))
                }

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" workInfo faultInfo
                logger.Logf LogLevel.Debug "%O : changing status to %A" workInfo WorkItemStatus.Dequeued

                let updateOp = 
                    setWorkItemDequeued localWorkerId.Id DateTimeOffset.Now
                                        workInfo.DeliveryCount (faultInfo.ToEnum())

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
             send          : WorkItemMessage -> Task) = async { 

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

        do! send msg |> Async.AwaitTaskCorrect

        logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize size)
    }

    static member EnqueueBatch
            (clusterId : ClusterId, 
             logger    : ISystemLogger, 
             jobs      : CloudWorkItem[], 
             send      : WorkItemMessage seq -> Task) = async { 
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
        do! send messages |> Async.AwaitTaskCorrect
        logger.LogInfof 
            "Enqueued batched jobs of %d items for task %s, total size %s." 
            jobs.Length 
            headJob.Process.Id 
            (getHumanReadableByteSize size)
    }

[<AutoOpen>]
module private QueueUtils = 
    let tryDequeue account queueUri = 
        async {
            let! res = Sqs.dequeueWithAttributes account queueUri None
            match res with
            | Some (receiptHandle, body, attr) -> 
                let message = fromBase64<WorkItemMessage> body
                let receiveCount = 
                    if attr.ContainsKey "ApproximateReceiveCount" 
                    then int <| attr.["ApproximateReceiveCount"] 
                    else 0
                let attributes = 
                    {
                        QueueUri      = queueUri
                        ReceiptHandle = receiptHandle
                        ReceiveCount  = receiveCount
                    }
                return Some (message, attributes)
            | _ -> return None
        }

/// Queue client implementation
[<Sealed; AutoSerializable(false)>]
type internal Queue (clusterId : ClusterId, queueUri, logger : ISystemLogger) = 
    let account = clusterId.SQSAccount
    let queue   = SQSCloudQueue<WorkItemMessage>(queueUri, account) :> CloudQueue<WorkItemMessage>

    let send msg = queue.EnqueueAsync msg |> Async.StartAsTask :> Task
    let sendBatch msgs = queue.EnqueueBatchAsync msgs |> Async.StartAsTask :> Task
    
    let tryDequeue () = tryDequeue account queueUri |> Async.StartAsTask

    member __.GetMessageCountAsync() = Sqs.getCount account queueUri

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
        let! queueUri = Sqs.tryGetQueueUri account clusterId.WorkItemQueueName
        match queueUri with
        | Some queueUri ->
            logger.LogInfof "Queue %A already exists." clusterId.WorkItemQueueName
            return new Queue(clusterId, queueUri, logger)
        | None ->
            logger.LogInfof "Creating new ServiceBus queue %A" clusterId.WorkItemQueueName
            // TODO : what should be the default queue attributes?
            let! queueUri = Sqs.createQueue account clusterId.WorkItemQueueName
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
    let tryDequeue () = 
        Sqs.tryGetQueueUri account queueName
        |> AsyncOption.Bind (tryDequeue account)
        |> Async.StartAsTask

    let tryGetCount = Sqs.getCount account |> AsyncOption.Lift

    member __.TargetWorkerId = targetWorkerId

    member __.GetMessageCountAsync() =
        Sqs.tryGetQueueUri account queueName
        |> AsyncOption.Bind tryGetCount
        |> AsyncOption.WithDefault 0L

    member __.TryDequeue(currentWorker : IWorkerId) = 
        MessagingClient.TryDequeue(clusterId, logger, currentWorker, tryDequeue)

    member __.DequeueAllMessagesBatch() = async {
        let! uri = Sqs.tryGetQueueUri account queueName
        match uri with
        | Some queueUri ->
            let! msgs = Sqs.dequeueAll account queueUri
            return msgs |> Array.map fromBase64<WorkItemMessage>
        | _ -> return [||]
    }

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
        let! queueUri = Sqs.tryGetQueueUri account queueName
        match queueUri with
        | Some uri -> return uri
        | _ -> return! Sqs.createQueue account queueName
    }

    let enqueue (msg : WorkItemMessage) = 
        async {
            let! queueUri = getQueueUriOrCreate msg
            let body = toBase64 msg
            do! Sqs.enqueue account queueUri body
        } 
        |> Async.StartAsTask
        :> Task

    // WorkItemQueue doesn't support mixed messages right now, so it's safe
    // to assume all messages in a batch have the same TargetWorker
    let enqueueBatch (msgs : seq<WorkItemMessage>) =
        async {
            if msgs |> Seq.isEmpty |> not then
                let! queueUri = getQueueUriOrCreate (Seq.head msgs)
                let msgBodies = msgs |> Seq.map toBase64
                do! Sqs.enqueueBatch account queueUri msgBodies
        }
        |> Async.StartAsTask
        :> Task

    member this.GetSubscription(subscriptionId : IWorkerId) = 
        new Subscription(clusterId, subscriptionId, logger)
        
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, enqueueBatch)
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, enqueue)

    member __.Delete(workerId : IWorkerId) = async {
        let queueName = getQueueName topic workerId.Id
        let! queueUri = Sqs.tryGetQueueUri account queueName
        match queueUri with
        | Some queueUri -> do! Sqs.deleteQueue account queueUri
        | _ -> return ()
    }

    static member Create(clusterId, logger : ISystemLogger) = 
        new Topic(clusterId, logger)