namespace MBrace.AWS.Runtime

open System

open Amazon.DynamoDBv2.DocumentModel

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.AWS.Runtime.Utilities

/// Blob payload of serialized work items 
type MessagePayload =
    | Single of CloudWorkItem
    | Batch  of CloudWorkItem []

type internal WorkItemKind =
    | ProcessRoot = 1
    | Parallel    = 2
    | Choice      = 3

type internal WorkItemStatus =
    | Preparing = 0
    | Enqueued  = 1
    | Dequeued  = 2
    | Started   = 3
    | Completed = 4
    | Faulted   = 5

type internal FaultInfo =
    | NoFault                            = 0
    | FaultDeclaredByWorker              = 1
    | WorkerDeathWhileProcessingWorkItem = 2
    | IsTargetedWorkItemOfDeadWorker     = 3

type WorkItemRecord(processId : string, workItemId : string) = 
    inherit DynamoDBTableEntity(processId, workItemId)

    member val Id             = workItemId with get
    member val ProcessId      = processId with get

    member val Affinity       = null : string with get, set
    member val Kind           = Nullable<int>() with get, set
    member val Index          = Nullable<int>() with get, set
    member val MaxIndex       = Nullable<int>() with get, set

    member val CurrentWorker  = null : string with get, set
    member val Status         = Nullable<int>() with get, set

    member val Size           = Nullable<int64>() with get, set
    member val EnqueueTime    = Nullable<DateTimeOffset>() with get, set
    member val DequeueTime    = Nullable<DateTimeOffset>() with get, set
    member val StartTime      = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime = Nullable<DateTimeOffset>() with get, set
    member val RenewLockTime  = Nullable<DateTimeOffset>() with get, set

    member val DeliveryCount  = Nullable<int>() with get, set
    member val Completed      = Nullable<bool>() with get, set
    member val Type           = null : string with get, set
    member val LastException  = null : string with get, set
    member val FaultInfo      = Nullable<int>() with get, set

    member val ETag           = null : string with get, set

    new () = new WorkItemRecord(null, null)

    member this.CloneDefault() =
        let p = new WorkItemRecord(processId, workItemId)
        p.ETag <- this.ETag
        p

    override this.ToString() = sprintf "workItem:%A" workItemId

    static member FromCloudWorkItem(workItem : CloudWorkItem) =
        let record = new WorkItemRecord(workItem.Process.Id, fromGuid workItem.Id)
        
        match workItem.WorkItemType with
        | ProcessRoot -> 
            record.Kind <- nullable(int WorkItemKind.ProcessRoot)
        | ParallelChild(i,m) ->
            record.Kind     <- nullable(int WorkItemKind.Parallel)
            record.Index    <- nullable i
            record.MaxIndex <- nullable m
        | ChoiceChild(i,m) ->
            record.Kind     <- nullable(int WorkItemKind.Choice)
            record.Index    <- nullable i
            record.MaxIndex <- nullable m
        
        match workItem.TargetWorker with
        | Some worker -> record.Affinity <- worker.Id
        | _ -> ()

        record.Status        <- nullable(int WorkItemStatus.Preparing)
        record.DeliveryCount <- nullable 0
        record.Completed     <- nullable false
        record.Type          <- PrettyPrinters.Type.prettyPrintUntyped workItem.Type
        record.FaultInfo     <- nullable(int FaultInfo.NoFault)
        record

    member r.GetWorkItemType() =
        let wk = enum<WorkItemKind>(r.Kind.GetValueOrDefault(-1))
        match wk with
        | WorkItemKind.ProcessRoot -> ProcessRoot
        | WorkItemKind.Choice   -> ChoiceChild(r.Index.GetValueOrDefault(-1), r.MaxIndex.GetValueOrDefault(-1))
        | WorkItemKind.Parallel -> ParallelChild(r.Index.GetValueOrDefault(-1), r.MaxIndex.GetValueOrDefault(-1))
        | _ -> failwithf "Invalid WorkItemKind %d" <| int wk

    member r.GetSize() = r.Size.GetValueOrDefault(-1L)

    static member FromDynamoDBDocument (doc : Document) =
        let processId  = doc.["ProcessId"].AsString()
        let workItemId = doc.["Id"].AsString()

        let record = new WorkItemRecord(processId, workItemId)

        record.Affinity      <- Table.readStringOrDefault doc "Affinity"
        record.Type          <- Table.readStringOrDefault doc "Type"
        record.ETag          <- Table.readStringOrDefault doc "ETag"
        record.CurrentWorker <- Table.readStringOrDefault doc "CurrentWorker"
        record.LastException <- Table.readStringOrDefault doc "LastException"

        record.Kind  <- Table.readIntOrDefault doc "Kind"
        record.Index <- Table.readIntOrDefault doc "Index"
        record.Size  <- Table.readInt64OrDefault doc "Size"
        record.MaxIndex  <- Table.readIntOrDefault doc "MaxIndex"
        record.Status    <- Table.readIntOrDefault doc "Status"
        record.Completed <- Table.readBoolOrDefault doc "Completed"
        record.FaultInfo <- Table.readIntOrDefault doc "FaultInfo"
        record.DeliveryCount <- Table.readIntOrDefault doc "DeliveryCount"

        record.EnqueueTime    <- Table.readDateTimeOffsetOrDefault doc "EnqueueTime"
        record.DequeueTime    <- Table.readDateTimeOffsetOrDefault doc "DequeueTime"
        record.StartTime      <- Table.readDateTimeOffsetOrDefault doc "StartTime"
        record.CompletionTime <- Table.readDateTimeOffsetOrDefault doc "CompletionTime"
        record.RenewLockTime  <- Table.readDateTimeOffsetOrDefault doc "RenewLockTime"

        record

    interface IDynamoDBDocument with 
        member this.ToDynamoDBDocument () =
            let doc = new Document()

            doc.["HashKey"]   <- DynamoDBEntry.op_Implicit(this.HashKey)
            doc.["RangeKey"]  <- DynamoDBEntry.op_Implicit(this.RangeKey)

            doc.["Id"]        <- DynamoDBEntry.op_Implicit(this.Id)
            doc.["ProcessId"] <- DynamoDBEntry.op_Implicit(this.ProcessId)
            doc.["Affinity"]  <- DynamoDBEntry.op_Implicit(this.Affinity)
            doc.["Type"]      <- DynamoDBEntry.op_Implicit(this.Type)
            doc.["ETag"]      <- DynamoDBEntry.op_Implicit(this.ETag)
            doc.["CurrentWorker"] <- DynamoDBEntry.op_Implicit(this.CurrentWorker)
            doc.["LastException"] <- DynamoDBEntry.op_Implicit(this.LastException)

            this.Kind  |> doIfNotNull (fun x -> doc.["Kind"] <- DynamoDBEntry.op_Implicit x)
            this.Index |> doIfNotNull (fun x -> doc.["Index"] <- DynamoDBEntry.op_Implicit x)
            this.Size  |> doIfNotNull (fun x -> doc.["Size"] <- DynamoDBEntry.op_Implicit x)
            this.MaxIndex  |> doIfNotNull (fun x -> doc.["MaxIndex"] <- DynamoDBEntry.op_Implicit x)
            this.Status    |> doIfNotNull (fun x -> doc.["Status"] <- DynamoDBEntry.op_Implicit x)
            this.Completed |> doIfNotNull (fun x -> doc.["Completed"] <- DynamoDBEntry.op_Implicit x)
            this.FaultInfo |> doIfNotNull (fun x -> doc.["FaultInfo"] <- DynamoDBEntry.op_Implicit x)

            this.EnqueueTime |> doIfNotNull (fun x -> doc.["EnqueueTime"] <- DynamoDBEntry.op_Implicit x)
            this.DequeueTime |> doIfNotNull (fun x -> doc.["DequeueTime"] <- DynamoDBEntry.op_Implicit x)
            this.StartTime   |> doIfNotNull (fun x -> doc.["StartTime"] <- DynamoDBEntry.op_Implicit x)
            this.CompletionTime |> doIfNotNull (fun x -> doc.["CompletionTime"] <- DynamoDBEntry.op_Implicit x)
            this.RenewLockTime  |> doIfNotNull (fun x -> doc.["RenewLockTime"] <- DynamoDBEntry.op_Implicit x)
            this.DeliveryCount  |> doIfNotNull (fun x -> doc.["DeliveryCount"] <- DynamoDBEntry.op_Implicit x)

            doc