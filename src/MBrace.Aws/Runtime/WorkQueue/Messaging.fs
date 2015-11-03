namespace MBrace.Aws.Runtime

open System
open System.IO
open System.Threading.Tasks

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities

type Message =
    {
        BlobUri      : string
        WorkItemId   : CloudWorkItemId
        ProcessId    : string
        TargetWorker : IWorkerId option
        BatchIndex   : int option
    }

type internal MessagingClient =

    static member Enqueue 
            (clusterId     : ClusterId, 
             logger        : ISystemLogger, 
             workItem      : CloudWorkItem, 
             allowNewSifts : bool, 
             send          : Message -> Task) = async { 
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
        let msg = 
            { 
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker
                BatchIndex   = None
            }
        do! send msg

        logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize size)
    }

    static member EnqueueBatch
            (clusterId : ClusterId, 
             logger    : ISystemLogger, 
             jobs      : CloudWorkItem[], 
             send      : Message seq -> Task) = async { 
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
        let mkWorkItemMessage (i : int) (workItem : CloudWorkItem) =
            {
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker
                BatchIndex   = Some i
            }

        let messages = jobs |> Array.mapi mkWorkItemMessage
        do! send messages
        logger.Logf LogLevel.Info "Enqueued batched jobs of %d items for task %s, total size %s." jobs.Length headJob.Process.Id (getHumanReadableByteSize size)
    }

/// Topic client implementation
/// TODO : implement on top of SNS
[<Sealed; AutoSerializable(false)>]
type internal Topic (clusterId : ClusterId, logger : ISystemLogger) = 
    let topic = clusterId.ServiceBusAccount.CreateTopicClient(clusterId.WorkItemTopic)

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
type internal Queue (clusterId : ClusterId, logger : ISystemLogger) = 
    let account  = clusterId.SQSAccount
    let queueUri = clusterId.WorkItemQueue

    member this.GetMessageCountAsync() = Sqs.getCount account queueUri

    member this.EnqueueBatch(jobs : CloudWorkItem []) = 
        failwith "not implemented yet"
            
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        MessagingClient.Enqueue(clusterId, logger, workItem, allowNewSifts, queue.SendAsync)
    
    member this.TryDequeue(workerId : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        failwith "not implemented yet"

    member this.EnqueueMessagesBatch(messages : seq<byte[]>) = 
        failwith "not implemented yet"
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = 
        failwith "not implemented yet"