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
        do! Table.update clusterId.DynamoDBAccount clusterId.RuntimeTable record
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
        do! Table.update clusterId.DynamoDBAccount clusterId.RuntimeTable newRecord

        // Step 4: send work item message to service bus queue
        let msg = 
            { 
                BlobUri      = blobUri
                WorkItemId   = workItem.Id
                ProcessId    = workItem.Process.Id
                TargetWorker = workItem.TargetWorker 
            }
        do! send msg

        logger.Logf LogLevel.Debug "workItem:%O : enqueue completed, size %s" workItem.Id (getHumanReadableByteSize size)
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