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
        WorkItemId : string

        TargetWorker : string option
        Type : CloudWorkItemType
        Status : WorkItemStatus
        TypeName : string
        Size : int64

        CurrentWorker : string option
        FaultInfo : FaultInfo

        [<FsPicklerJson>]
        LastException : ExceptionDispatchInfo option

        EnqueueTime : DateTimeOffset
        DequeueTime : DateTimeOffset option
        StartTime : DateTimeOffset option
        CompletionTime : DateTimeOffset option
        RenewLockTime : DateTimeOffset option
        DeliveryCount : int
        Completed : bool
    }
with
    static member GetHashKey (procId) = "cloudProcess:" + procId
    static member GetRangeKey (workItemId : Guid) = "workItem:" + workItemId.ToString()

    static member FromCloudWorkItem(workItem : CloudWorkItem, size : int64) =
        {
            ProcessId = WorkItemRecord.GetHashKey workItem.Process.Id
            WorkItemId = WorkItemRecord.GetRangeKey workItem.Id
            TargetWorker = workItem.TargetWorker |> Option.map (fun w -> w.Id)
            Type = workItem.WorkItemType
            Status = WorkItemStatus.Enqueued
            TypeName = PrettyPrinters.Type.prettyPrintUntyped workItem.Type
            FaultInfo = FaultInfo.NoFault
            Size = size
            CurrentWorker = None
            LastException = None
            EnqueueTime = DateTimeOffset.Now
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

    type CloudWorkItemFaultInfo with
        member fI.ToEnum() =
            match fI with
            | NoFault -> FaultInfo.NoFault
            | IsTargetedWorkItemOfDeadWorker _ -> FaultInfo.IsTargetedWorkItemOfDeadWorker
            | FaultDeclaredByWorker _ -> FaultInfo.FaultDeclaredByWorker
            | WorkerDeathWhileProcessingWorkItem _ -> FaultInfo.WorkerDeathWhileProcessingWorkItem

    let setWorkItemCompleted =
        <@ fun t (r:WorkItemRecord) -> 
                    SET r.CompletionTime.Value t &&& 
                    SET r.Completed true &&& 
                    SET r.Status WorkItemStatus.Completed @>

        |> template.PrecomputeUpdateExpr

    let setWorkItemFaulted =
        <@ fun edi t (r:WorkItemRecord) -> 
                    SET r.CompletionTime.Value t &&&
                    SET r.LastException edi &&& 
                    SET r.FaultInfo FaultInfo.FaultDeclaredByWorker &&& 
                    SET r.Status WorkItemStatus.Faulted @>

        |> template.PrecomputeUpdateExpr

    let setWorkItemDequeued =
        <@ fun w t c f (r:WorkItemRecord) ->
                SET r.DequeueTime.Value t &&&
                SET r.Status WorkItemStatus.Dequeued &&&
                SET r.CurrentWorker.Value w &&&
                SET r.DeliveryCount c &&&
                SET r.FaultInfo f @>

        |> template.PrecomputeUpdateExpr

    let getWorkItemsByProcessQuery =
        let qc =
            <@ fun procId (r:WorkItemRecord) -> r.ProcessId = procId @>
            |> template.PrecomputeConditionalExpr

        fun pid -> qc (WorkItemRecord.GetHashKey pid)