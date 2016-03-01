namespace MBrace.AWS.Runtime

open System

open FSharp.DynamoDB

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
        ReturnType : string
        Size : int64

        CurrentWorker : string option
        FaultInfo : FaultInfo
        LastException : string option
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
            ReturnType = PrettyPrinters.Type.prettyPrintUntyped workItem.Type
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