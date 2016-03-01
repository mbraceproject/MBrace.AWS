namespace MBrace.AWS.Runtime

open System
open System.Runtime.Serialization

open FSharp.DynamoDB

open Nessos.FsPickler
open Nessos.Vagabond

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.AWS
open MBrace.AWS.Runtime.Utilities

[<AutoOpen>]
module private ProcessEntryImpl =

    [<ConstantHashKey("HashKey", "CloudProcess")>]
    type CloudProcessRecord =
        {
            [<RangeKey; CustomName("RangeKey")>]
            Id : string

            Name : string option
            Status : CloudProcessStatus
            EnqueuedTime : DateTimeOffset
            DequeuedTime : DateTimeOffset option
            StartTime : DateTimeOffset option
            CompletionTime : DateTimeOffset option
            Completed : bool

            [<FsPicklerJson>]
            CancellationTokenSource : ICloudCancellationTokenSource
            [<FsPicklerBinary>]
            Dependencies : AssemblyId []
            [<FsPicklerBinary>]
            AdditionalResources : ResourceRegistry option

            ResultUri : string option
            TypeName : string
            Type : byte[]
        }
    with
        static member CreateNew(taskId : string, info : CloudProcessInfo) =
            {
                Id = taskId
                Completed = false
                StartTime = None
                CompletionTime = None
                Dependencies = info.Dependencies
                EnqueuedTime = DateTimeOffset.Now
                DequeuedTime = None
                Name = info.Name
                Status = CloudProcessStatus.Created
                Type = info.ReturnType.Bytes
                TypeName = info.ReturnTypeName
                CancellationTokenSource = info.CancellationTokenSource
                AdditionalResources = info.AdditionalResources
                ResultUri = None
            }

        static member GetProcessRecord(clusterId : ClusterId, processId : string) = async {
            let key = TableKey.Range processId
            return! clusterId.GetRuntimeTable<CloudProcessRecord>().GetItemAsync(key)
        }

        member record.ToCloudProcessInfo() =
            {
                Name = record.Name
                CancellationTokenSource = record.CancellationTokenSource
                Dependencies = record.Dependencies
                AdditionalResources = record.AdditionalResources
                ReturnTypeName = record.TypeName
                ReturnType = new Pickle<_>(record.Type)
            }

    let private ptemplate = template<CloudProcessRecord>
    let private wtemplate = template<WorkItemRecord>

    let setEnqueued =
        <@ fun t (r:CloudProcessRecord) -> { r with EnqueuedTime = t ; Completed = false } @>
        |> ptemplate.PrecomputeUpdateExpr 

    let setDequeued =
        <@ fun t (r:CloudProcessRecord) -> { r with DequeuedTime = t } @>
        |> ptemplate.PrecomputeUpdateExpr 

    let setStarted =
        <@ fun t (r:CloudProcessRecord) -> { r with StartTime = t } @>
        |> ptemplate.PrecomputeUpdateExpr

    let setCompleted =
        <@ fun t (r:CloudProcessRecord) -> { r with CompletionTime = t ; Completed = true } @>
        |> ptemplate.PrecomputeUpdateExpr

    let setProcessResult =
        <@ fun u (r:CloudProcessRecord) -> { r with ResultUri = u } @>
        |> ptemplate.PrecomputeUpdateExpr

    let resultUnsetPrecondition =
        <@ fun (r:CloudProcessRecord) -> r.ResultUri = None @>
        |> ptemplate.PrecomputeConditionalExpr

    let jobQueryExpr =
        <@ fun pid (r:WorkItemRecord) -> r.ProcessId = pid @>
        |> wtemplate.PrecomputeConditionalExpr

[<DataContract; Sealed>]
type internal CloudProcessEntry (clusterId : ClusterId, processId : string, processInfo : CloudProcessInfo) =
    [<DataMember(Name = "ClusterId")>]
    let clusterId = clusterId

    [<DataMember(Name = "ProcessId")>] 
    let processId = processId

    [<DataMember(Name = "ProcessInfo")>]
    let processInfo = processInfo

    let key() = TableKey.Range processId
    let getProcTable() = clusterId.GetRuntimeTable<CloudProcessRecord>()
    let getJobTable() = clusterId.GetRuntimeTable<WorkItemRecord>()

    override this.ToString() = sprintf "task:%A" processId

    interface ICloudProcessEntry with
        member this.Id: string = processId
        member this.Info: CloudProcessInfo = processInfo

        member this.AwaitResult(): Async<CloudProcessResult> = async {
            let entry   = this :> ICloudProcessEntry
            let! result = entry.TryGetResult()
            match result with
            | Some r -> return r
            | None ->
                do! Async.Sleep 200
                return! entry.AwaitResult()
        }

        member this.WaitAsync(): Async<unit> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, processId)
            // result uri has been populated, hence computation has completed
            if Option.isNone record.ResultUri then return ()
            else
                do! Async.Sleep 200
                return! (this :> ICloudProcessEntry).WaitAsync()
        }
        
        member this.IncrementCompletedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementFaultedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementWorkItemCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = async {
            let now = DateTimeOffset.Now
            let uExpr =
                match status with
                | CloudProcessStatus.Created -> setEnqueued now
                | CloudProcessStatus.WaitingToRun -> setDequeued (Some now)
                | CloudProcessStatus.Running -> setStarted (Some now)
                | CloudProcessStatus.Faulted
                | CloudProcessStatus.Completed
                | CloudProcessStatus.UserException
                | CloudProcessStatus.Canceled -> setCompleted (Some now)
                | _ -> invalidArg "status" "invalid Cloud process status."

            let! _ = getProcTable().UpdateItemAsync(key(), uExpr)
            return ()
        }
        
        member this.GetState(): Async<CloudProcessState> = async {
            let! jobsHandle = getJobTable().QueryAsync (jobQueryExpr processId) |> Async.StartChild
            let! record = getProcTable().GetItemAsync(key())
            let! jobs = jobsHandle

            let execTime =
                match record.Completed, record.StartTime, record.CompletionTime with
                | true, Some s, Some c ->
                    Finished(s, c - s)
                | false, Some s, _ ->
                    Started(s, DateTimeOffset.Now - s)
                | false, None, None -> NotStarted
                | _ -> 
                    let ex = new InvalidOperationException(sprintf "Invalid record %s" record.Id)
                    ex.Data.Add("record", record)
                    raise ex

            let total = jobs.Length
            let active, completed, faulted =
                jobs
                |> Seq.fold (fun ((a,c,f) as state) workItem ->
                    match workItem.Status with
                    | WorkItemStatus.Preparing 
                    | WorkItemStatus.Enqueued  -> state
                    | WorkItemStatus.Faulted   -> (a, c, f + 1)
                    | WorkItemStatus.Dequeued
                    | WorkItemStatus.Started   -> (a + 1, c, f)
                    | WorkItemStatus.Completed -> (a, c + 1, f)
                    | _ as s -> failwithf "Invalid WorkItemStatus %A" s) (0, 0, 0)

            return 
                { 
                    Status = record.Status
                    Info   = (this :> ICloudProcessEntry).Info
                    ExecutionTime = execTime // TODO : dequeued vs running time?
                    ActiveWorkItemCount    = active
                    CompletedWorkItemCount = completed
                    FaultedWorkItemCount   = faulted
                    TotalWorkItemCount     = total 
                }
        }

        member this.TryGetResult(): Async<CloudProcessResult option> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, processId)
            match record.ResultUri with
            | None -> return None
            | Some uri ->
                let! result = S3Persist.ReadPersistedClosure<CloudProcessResult>(clusterId, uri)
                return Some result
        }

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = async {
            let blobUri = guid()
            do! S3Persist.PersistClosure(clusterId, result, blobUri, allowNewSifts = false)
            try
                let! _ = getProcTable().UpdateItemAsync(key(), setProcessResult (Some blobUri), precondition = resultUnsetPrecondition)
                return true
            with :? ConditionalCheckFailedException -> return false
        }