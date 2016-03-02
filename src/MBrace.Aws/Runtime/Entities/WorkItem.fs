namespace MBrace.AWS.Runtime

open System

open FSharp.DynamoDB

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.AWS.Runtime.Utilities

/// Blob payload of serialized work items 
type MessagePayload =
    | Single of CloudWorkItem
    | Batch  of CloudWorkItem []

type WorkItemStatus =
    | Preparing = 0
    | Enqueued  = 1
    | Dequeued  = 2
    | Started   = 3
    | Completed = 4
    | Faulted   = 5

type FaultInfo =
    | NoFault                            = 0
    | FaultDeclaredByWorker              = 1
    | WorkerDeathWhileProcessingWorkItem = 2
    | IsTargetedWorkItemOfDeadWorker     = 3

type WorkItemRecord =
    {
        [<HashKey; CustomName("HashKey")>]
        ProcessId : string

        [<RangeKey; CustomName("RangeKey")>]
        WorkItemId : Guid

        TargetWorker : string option
        Type : CloudWorkItemType
        Status : WorkItemStatus
        TypeName : string
        Size : int64

        CurrentWorker : string option
        FaultInfo : FaultInfo

        [<FsPicklerJson>]
        LastException : ExceptionDispatchInfo option

        EnqueueTime : DateTimeOffset option
        DequeueTime : DateTimeOffset option
        StartTime : DateTimeOffset option
        CompletionTime : DateTimeOffset option
        RenewLockTime : DateTimeOffset option
        DeliveryCount : int
        Completed : bool
    }
with
    static member FromCloudWorkItem(workItem : CloudWorkItem) =
        {
            ProcessId = workItem.Process.Id
            WorkItemId = workItem.Id
            TargetWorker = workItem.TargetWorker |> Option.map (fun w -> w.Id)
            Type = workItem.WorkItemType
            Status = WorkItemStatus.Preparing
            TypeName = PrettyPrinters.Type.prettyPrintUntyped workItem.Type
            FaultInfo = FaultInfo.NoFault
            Size = 0L
            CurrentWorker = None
            LastException = None
            EnqueueTime = None
            DequeueTime = None
            StartTime = None
            CompletionTime = None
            RenewLockTime = None
            DeliveryCount = 0
            Completed = false
        }

[<AutoOpen>]
module internal WorkItemRecordImpl =
    
    let private template = template<WorkItemRecord>

    let setWorkItemCompleted =
        <@ fun t (r:WorkItemRecord) -> 
                    SET r.CompletionTime.Value t &&& 
                    SET r.Completed true &&& 
                    SET r.Status WorkItemStatus.Completed @>

        |> template.PrecomputeUpdateExpr

    let setWorkItemFaulted =
        <@ fun edi t (r:WorkItemRecord) -> 
                    SET r.CompletionTime.Value t &&&
                    SET r.LastException.Value edi &&& 
                    SET r.FaultInfo FaultInfo.FaultDeclaredByWorker &&& 
                    SET r.Status WorkItemStatus.Faulted @>

        |> template.PrecomputeUpdateExpr