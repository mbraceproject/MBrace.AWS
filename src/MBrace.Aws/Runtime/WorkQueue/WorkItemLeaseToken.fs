namespace MBrace.AWS.Runtime

open System
open System.IO
open System.Threading
open System.Runtime.Serialization

open Microsoft.FSharp.Control

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

open Amazon.SQS.Model
open FSharp.DynamoDB

open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

type WorkItemMessage = 
    {
        ProcessId    : string
        WorkItemId   : CloudWorkItemId
        BatchIndex   : int option
        TargetWorker : string option
        BlobUri      : string
    }

type WorkItemMessageAttributes =
    {
        QueueUri      : string
        ReceiptHandle : string
        ReceiveCount  : int
    }

type internal WorkItemLeaseTokenInfo =
    {
        QueueUri      : string
        ReceiptHandle : string
        ProcessId     : string
        WorkItemId    : Guid
        BatchIndex    : int option
        TargetWorker  : string option
        BlobUri       : string
        DequeueTime   : DateTimeOffset
        DeliveryCount : int
    }

    override this.ToString() = sprintf "leaseinfo:%A" this.WorkItemId

    member internal __.TableKey = 
        TableKey.Combined(__.ProcessId, __.WorkItemId)

    static member FromReceivedMessage(message : WorkItemMessage, attributes : WorkItemMessageAttributes) =
        {
            QueueUri      = attributes.QueueUri
            ReceiptHandle = attributes.ReceiptHandle
            WorkItemId    = message.WorkItemId
            BlobUri       = message.BlobUri
            ProcessId     = message.ProcessId
            BatchIndex    = message.BatchIndex
            TargetWorker  = message.TargetWorker
            DequeueTime   = DateTimeOffset.Now
            DeliveryCount = attributes.ReceiveCount
        }

type internal LeaseAction =
    | Complete
    | Abandon

/// Periodically renews lock for supplied work item, releases lock if specified as completed.
[<Sealed; AutoSerializable(false)>]
type internal WorkItemLeaseMonitor private 
        (clusterId : ClusterId,
         info      : WorkItemLeaseTokenInfo,
         logger    : ISystemLogger) =

    let deleteMsg () = async {
        let req = DeleteMessageRequest(
                    QueueUrl      = info.QueueUri,
                    ReceiptHandle = info.ReceiptHandle)
        let! ct = Async.CancellationToken
        do! clusterId.SQSAccount.SQSClient.DeleteMessageAsync(req, ct)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    let rec renewLoop (inbox : MailboxProcessor<LeaseAction>) = async {
        let! action = inbox.TryReceive(timeout = 60)
        match action with
        | None ->
            let req = ChangeMessageVisibilityRequest(
                         QueueUrl      = info.QueueUri,
                         ReceiptHandle = info.ReceiptHandle)
            req.VisibilityTimeout <- 60 // hide message from other workers for another 1 min

            let! ct = Async.CancellationToken
            let! res = clusterId.SQSAccount.SQSClient.ChangeMessageVisibilityAsync(req, ct)
                       |> Async.AwaitTaskCorrect
                       |> Async.Catch

            match res with
            | Choice1Of2 _ -> 
                logger.Logf LogLevel.Debug "%A : lock renewed" info
                return! renewLoop inbox
            | Choice2Of2 (:? ReceiptHandleIsInvalidException) ->
                logger.Logf LogLevel.Warning "%A : lock lost" info
            | Choice2Of2 exn -> 
                logger.LogError <| sprintf "%A : lock renew failed with %A" info exn
                return! renewLoop inbox

        | Some Complete ->
            do! deleteMsg()
            logger.LogInfof "%A : completed" info

        | Some Abandon ->
            do! deleteMsg()
            logger.LogInfof "%A : abandoned" info
    }

    let cts = new CancellationTokenSource()
    let mbox = MailboxProcessor.Start(renewLoop, cts.Token)

    member __.CompleteWith(action) = mbox.Post action

    interface IDisposable with 
        member __.Dispose() = cts.Cancel()

    static member Start(id : ClusterId, info : WorkItemLeaseTokenInfo, logger : ISystemLogger) =
        new WorkItemLeaseMonitor(id, info, logger)

/// Implements ICloudWorkItemLeaseToken
type internal WorkItemLeaseToken =
    {
        ClusterId       : ClusterId
        CompleteAction  : MarshaledAction<LeaseAction> // ensures that LeaseMonitor is serializable across AppDomains
        WorkItemType    : CloudWorkItemType
        WorkItemSize    : int64
        TypeName        : string
        FaultInfo       : CloudWorkItemFaultInfo
        LeaseInfo       : WorkItemLeaseTokenInfo
        ProcessInfo     : CloudProcessInfo
    }

    member private __.Table = __.ClusterId.GetRuntimeTable<WorkItemRecord>()
    
    interface ICloudWorkItemLeaseToken with
        member this.DeclareCompleted() : Async<unit> = async {
            this.CompleteAction.Invoke Complete
            this.CompleteAction.Dispose() // disconnect marshaled object

            let! _ = this.Table.UpdateItemAsync(this.LeaseInfo.TableKey, setWorkItemCompleted DateTimeOffset.Now)
            return ()
        }
        
        member this.DeclareFaulted(edi : ExceptionDispatchInfo) : Async<unit> = async {
            this.CompleteAction.Invoke Abandon
            this.CompleteAction.Dispose() // disconnect marshaled object

            let! _ = this.Table.UpdateItemAsync(this.LeaseInfo.TableKey, setWorkItemFaulted edi DateTimeOffset.Now)
            return ()
        }
        
        member this.FaultInfo : CloudWorkItemFaultInfo = this.FaultInfo
        
        member this.GetWorkItem() : Async<CloudWorkItem> = async { 
            let! payload = S3Persist.ReadPersistedClosure<MessagePayload>(this.ClusterId, this.LeaseInfo.BlobUri)
            match payload with
            | Single item -> return item
            | Batch items -> return items.[Option.get this.LeaseInfo.BatchIndex]
        }
        
        member this.Id : CloudWorkItemId = this.LeaseInfo.WorkItemId
        
        member this.WorkItemType : CloudWorkItemType = this.WorkItemType
        
        member this.Size : int64 = this.WorkItemSize
        
        member this.TargetWorker : IWorkerId option = 
            match this.LeaseInfo.TargetWorker with
            | None   -> None
            | Some w -> Some(WorkerId(w) :> _)
        
        member this.Process : ICloudProcessEntry = 
            new CloudProcessEntry(this.ClusterId, this.LeaseInfo.ProcessId, this.ProcessInfo) :> _
        
        member this.Type : string = this.TypeName

    /// Creates a new WorkItemLeaseToken with supplied configuration parameters
    static member Create
            (clusterId : ClusterId, 
             info      : WorkItemLeaseTokenInfo, 
             monitor   : WorkItemLeaseMonitor, 
             faultInfo : CloudWorkItemFaultInfo) = async {

        let! processRecordT = 
            CloudProcessRecord.GetProcessRecord(clusterId, info.ProcessId) 
            |> Async.StartChild

        let! workRecord = 
            clusterId.GetRuntimeTable<WorkItemRecord>()
                     .GetItemAsync(info.TableKey)

        let! processRecord = processRecordT

        return {
                    ClusterId      = clusterId
                    CompleteAction = MarshaledAction.Create monitor.CompleteWith
                    WorkItemSize   = workRecord.Size
                    WorkItemType   = workRecord.Type
                    TypeName       = workRecord.TypeName
                    FaultInfo      = faultInfo
                    LeaseInfo      = info
                    ProcessInfo    = processRecord.ToCloudProcessInfo()
               }
    }