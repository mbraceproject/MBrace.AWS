namespace MBrace.Aws.Runtime

open System
open System.IO
open System.Threading.Tasks

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities
open MBrace.Aws.Store

type internal MessagingClient =
    static member TryDequeue 
            (clusterId     : ClusterId, 
             logger        : ISystemLogger, 
             localWorkerId : IWorkerId, 
             dequeue       : unit -> Task<(WorkItemMessage * WorkItemMessageAttributes) option>) 
            : Async<ICloudWorkItemLeaseToken option> = async { 
        let! res = dequeue()
        match res with
        | None -> return None
        | Some (message, attributes) ->
            let workInfo = WorkItemLeaseTokenInfo.FromReceivedMessage(message, attributes)
            logger.Logf LogLevel.Debug "%O : dequeued, delivery count = %d" workInfo workInfo.DeliveryCount 

            logger.Logf LogLevel.Debug "%O : starting lock renew loop" workInfo
            let monitor = WorkItemLeaseMonitor.Start(clusterId, workInfo, logger)

            try
                let newRecord = new WorkItemRecord(workInfo.ProcessId, fromGuid workInfo.WorkItemId)
                newRecord.ETag          <- "*"
                newRecord.Completed     <- nullable false
                newRecord.DequeueTime   <- nullable workInfo.DequeueTime
                newRecord.Status        <- nullable(int WorkItemStatus.Dequeued)
                newRecord.CurrentWorker <- localWorkerId.Id
                newRecord.DeliveryCount <- nullable workInfo.DeliveryCount
                newRecord.FaultInfo     <- nullable(int FaultInfo.NoFault)

                // determine the fault info for the dequeued work item
                let! faultInfo = async {
                    let faultCount = workInfo.DeliveryCount - 1
                    match workInfo.TargetWorker with
                    | Some target when target <> localWorkerId.Id -> 
                        // a targeted work item that has been dequeued by a different worker is to be declared faulted
                        newRecord.FaultInfo <- nullable(int FaultInfo.IsTargetedWorkItemOfDeadWorker)
                        return IsTargetedWorkItemOfDeadWorker(faultCount, new WorkerId(target))

                    | _ when faultCount = 0 -> return NoFault
                    | _ ->
                        let hashKey    = workInfo.ProcessId
                        let rangeKey   = fromGuid workInfo.WorkItemId
                        let! oldRecord = Table.read<WorkItemRecord> clusterId.DynamoDBAccount clusterId.RuntimeTable hashKey rangeKey

                        match enum<FaultInfo> oldRecord.FaultInfo.Value with
                        | FaultInfo.FaultDeclaredByWorker -> // a fault exception has been set by the executing worker
                            let lastExc = ProcessConfiguration.JsonSerializer.UnPickleOfString<ExceptionDispatchInfo>(oldRecord.LastException)
                            let lastWorker = new WorkerId(oldRecord.CurrentWorker)
                            return FaultDeclaredByWorker(faultCount, lastExc, lastWorker)
                        | _ -> // a worker has died while previously dequeueing the worker
                            newRecord.FaultInfo <- nullable(int FaultInfo.WorkerDeathWhileProcessingWorkItem)
                            // account for cases where worker died before even updating the work item record
                            let previousWorker = 
                                match oldRecord.CurrentWorker with 
                                | null -> "<unknown>" 
                                | w    -> w
                            return WorkerDeathWhileProcessingWorkItem(faultCount, new WorkerId(previousWorker))
                }

                logger.Logf LogLevel.Debug "%O : extracted fault info %A" workInfo faultInfo
                logger.Logf LogLevel.Debug "%O : changing status to %A" workInfo WorkItemStatus.Dequeued
                do! Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable newRecord
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
        // Step 1: initial record entry creation
        let record = WorkItemRecord.FromCloudWorkItem(workItem)
        do! Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable record
        logger.Logf LogLevel.Debug "workItem:%O : enqueue" workItem.Id

        // Step 2: Persist work item payload to blob store
        let blobUri = sprintf "workItem/%s/%s" workItem.Process.Id (fromGuid workItem.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Single workItem, blobUri, allowNewSifts)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 3: update record entry
        let newRecord = record.CloneDefault()
        newRecord.Status      <- nullable(int WorkItemStatus.Enqueued)
        newRecord.EnqueueTime <- nullable DateTimeOffset.Now
        newRecord.Size        <- nullable size
        newRecord.FaultInfo   <- nullable(int FaultInfo.NoFault)
        newRecord.ETag        <- "*"
        do! Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable newRecord

        // Step 4: send work item message to service bus queue
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
             send      : WorkItemMessage seq -> Task) = async { 
        // silent discard if empty
        if jobs.Length = 0 then return () else

        // Step 1: initial work item record population
        let records = jobs |> Seq.map WorkItemRecord.FromCloudWorkItem
        do! Table.putBatch clusterId.DynamoDBAccount clusterId.RuntimeTable records

        // Step 2: persist payload to blob store
        let headJob = jobs.[0]
        let blobUri = sprintf "workItem/%s/batch/%s" headJob.Process.Id (fromGuid headJob.Id)
        do! S3Persist.PersistClosure<MessagePayload>(clusterId, Batch jobs, blobUri, allowNewSifts = false)
        let! size = S3Persist.GetPersistedClosureSize(clusterId, blobUri)

        // Step 3: update runtime records
        let now = DateTimeOffset.Now
        let newRecords = 
            records |> Seq.map (fun r -> 
                let newRec = r.CloneDefault()
                newRec.ETag        <- "*"
                newRec.Status      <- nullable(int WorkItemStatus.Enqueued)
                newRec.EnqueueTime <- nullable now
                newRec.FaultInfo   <- nullable(int FaultInfo.NoFault)
                newRec.Size        <- nullable(size)
                newRec)

        do! Table.putBatch clusterId.DynamoDBAccount clusterId.RuntimeTable newRecords

        // Step 4: create work messages and post to service bus queue
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
        logger.Logf LogLevel.Info "Enqueued batched jobs of %d items for task %s, total size %s." jobs.Length headJob.Process.Id (getHumanReadableByteSize size)
    }

/// Topic subscription client
[<Sealed; AutoSerializable(false)>]
type internal Subscription (clusterId : ClusterId, targetWorkerId : IWorkerId, logger : ISystemLogger) = 
    member this.TargetWorkerId = targetWorkerId

    member this.GetMessageCountAsync() = 
        failwith "not implemented yet"

    member this.TryDequeue(currentWorker : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        failwith "not implemented yet"

    member this.DequeueAllMessagesBatch() = 
        failwith "not implemented yet"

/// Topic client implementation
/// TODO : implement on top of SNS
[<Sealed; AutoSerializable(false)>]
type internal Topic (clusterId : ClusterId, logger : ISystemLogger) = 
    member this.GetMessageCountAsync() = 
        failwith "not implemented yet"

    member this.GetSubscription(subscriptionId : IWorkerId) : Subscription = 
        failwith "not implemented yet"
    
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        failwith "not implemented yet"
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        failwith "not implemented yet"

    static member Create(config, logger : ISystemLogger) = 
        failwith "not implemented yet"

/// Queue client implementation
[<Sealed; AutoSerializable(false)>]
type internal Queue (clusterId : ClusterId, queueUri, logger : ISystemLogger) = 
    let account = clusterId.SQSAccount
    let queue   = SQSQueue<WorkItemMessage>(queueUri, account) :> CloudQueue<WorkItemMessage>

    let send msg = queue.EnqueueAsync msg |> Async.StartAsTask :> Task
    let sendBatch msgs = queue.EnqueueBatchAsync msgs |> Async.StartAsTask :> Task

    let dequeue () = 
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
        |> Async.StartAsTask

    member this.GetMessageCountAsync() = Sqs.getCount account queueUri

    member this.EnqueueBatch(jobs : CloudWorkItem []) = 
        MessagingClient.EnqueueBatch(clusterId, logger, jobs, sendBatch)
            
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, send)
    
    member this.TryDequeue(workerId : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        MessagingClient.TryDequeue(clusterId, logger, workerId, dequeue)

    member this.EnqueueMessagesBatch(messages : seq<WorkItemMessage>) = async {
        do! queue.EnqueueBatchAsync(messages)
    }
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = async { 
        let account = clusterId.SQSAccount
        let! queueUri = Sqs.tryGetQueueUri account clusterId.WorkItemQueue
        match queueUri with
        | Some queueUri ->
            logger.Logf LogLevel.Info "Queue %A already exists." clusterId.WorkItemQueue
            return new Queue(clusterId, queueUri, logger)
        | None ->
            logger.Logf LogLevel.Info "Creating new ServiceBus queue %A" clusterId.WorkItemQueue
            // TODO : what should be the default queue attributes?

            let! queueUri = Sqs.createQueue account clusterId.WorkItemQueue
            return new Queue(clusterId, queueUri, logger)
    }