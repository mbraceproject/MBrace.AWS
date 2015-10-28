namespace MBrace.Aws.Runtime

open System
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Core
open MBrace.Runtime
open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

[<DataContract; Sealed>]
type internal CloudProcessEntry 
        (clusterId   : ClusterId, 
         processId   : string, 
         processInfo : CloudProcessInfo) =
    [<DataMember(Name = "ClusterId")>]
    let clusterId = clusterId

    [<DataMember(Name = "ProcessId")>] 
    let processId = processId

    [<DataMember(Name = "ProcessInfo")>]
    let processInfo = processInfo

    override this.ToString() = sprintf "task:%A" processId

    interface ICloudProcessEntry with
        member this.Id: string = processId

        member this.AwaitResult(): Async<CloudProcessResult> = async {
            let tcs = this :> ICloudProcessEntry
            let! result = tcs.TryGetResult()
            match result with
            | Some r -> return r
            | None ->
                do! Async.Sleep 200
                return! tcs.AwaitResult()
        }

        member this.WaitAsync(): Async<unit> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
            // result uri has been populated, hence computation has completed
            if record.ResultUri <> null then return ()
            else
                do! Async.Sleep 200
                return! (this :> ICloudProcessEntry).WaitAsync()
        }
        
        member this.IncrementCompletedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementFaultedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementWorkItemCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = async {
            let record = new CloudProcessRecord(taskId)
            record.Status <- nullable(int status)
            record.ETag <- "*"
            let now = nullable DateTimeOffset.Now
            match status with
            | CloudProcessStatus.Created -> 
                record.Completed <- nullable false
                record.EnqueuedTime <- now
            | CloudProcessStatus.WaitingToRun -> 
                record.Completed <- nullable false
                record.DequeuedTime <- now
            | CloudProcessStatus.Running -> 
                record.Completed <- nullable false
                record.StartTime <- now
            | CloudProcessStatus.Faulted
            | CloudProcessStatus.Completed
            | CloudProcessStatus.UserException
            | CloudProcessStatus.Canceled -> 
                record.Completed <- nullable true
                record.CompletionTime <- nullable DateTimeOffset.Now

            | _ -> invalidArg "status" "invalid Cloud process status."

            let! _ = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return ()
        }
        
        member this.GetState(): Async<CloudProcessState> = async {
            let! jobsHandle = Async.StartChild(Table.queryPK<WorkItemRecord> clusterId.StorageAccount clusterId.RuntimeTable taskId)
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
            let! jobs = jobsHandle

            let execTime =
                match record.Completed, record.StartTime, record.CompletionTime with
                | Nullable true, Nullable s, Nullable c ->
                    Finished(s, c - s)
                | Nullable false, Nullable s, _ ->
                    Started(s, DateTimeOffset.Now - s)
                | Nullable false, Null, Null ->
                    NotStarted
                | _ -> 
                    let ex = new InvalidOperationException(sprintf "Invalid record %s" record.Id)
                    ex.Data.Add("record", record)
                    raise ex

            let total = jobs.Count
            let active, completed, faulted =
                jobs
                |> Seq.fold (fun ((a,c,f) as state) workItem ->
                    match enum<WorkItemStatus> workItem.Status.Value with
                    | WorkItemStatus.Preparing 
                    | WorkItemStatus.Enqueued  -> state
                    | WorkItemStatus.Faulted   -> (a, c, f + 1)
                    | WorkItemStatus.Dequeued
                    | WorkItemStatus.Started   -> (a + 1, c, f)
                    | WorkItemStatus.Completed -> (a, c + 1, f)
                    | _ as s -> failwith "Invalid WorkItemStatus %A" s) (0, 0, 0)

            return 
                { 
                    Status = enum (record.Status.GetValueOrDefault(-1))
                    Info = (this :> ICloudProcessEntry).Info
                    ExecutionTime = execTime // TODO : dequeued vs running time?
                    ActiveWorkItemCount = active
                    CompletedWorkItemCount = completed
                    FaultedWorkItemCount = faulted
                    TotalWorkItemCount = total 
                }
        }

        member this.Info: CloudProcessInfo = processInfo
        
        member this.TryGetResult(): Async<CloudProcessResult option> = async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, taskId)
            match record.ResultUri with
            | null -> return None
            | uri ->
                let! result = BlobPersist.ReadPersistedClosure<CloudProcessResult>(clusterId, uri)
                return Some result
        }

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = async {
            let record = new CloudProcessRecord(taskId)
            let blobUri = guid()
            do! BlobPersist.PersistClosure(clusterId, result, blobUri, allowNewSifts = false)
            record.ResultUri <- blobUri
            record.ETag <- "*"
            let! _record = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return true
        }