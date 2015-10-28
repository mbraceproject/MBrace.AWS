namespace MBrace.Aws.Runtime

open System
open System.Runtime.Serialization

open Amazon.DynamoDBv2.DocumentModel

open Nessos.FsPickler

open MBrace.Core
open MBrace.Runtime
open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

[<AllowNullLiteral>]
type CloudProcessRecord(taskId) = 
    member val Id   : string = taskId with get, set
    member val Name : string = null with get, set

    member val Status         = Nullable<int>() with get, set
    member val EnqueuedTime   = Nullable<DateTimeOffset>() with get, set
    member val DequeuedTime   = Nullable<DateTimeOffset>() with get, set
    member val StartTime      = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime = Nullable<DateTimeOffset>() with get, set
    member val Completed      = Nullable<bool>() with get, set

    member val CancellationTokenSource : byte [] = null with get, set
    member val ResultUri : string = null with get, set
    member val TypeName  : string = null with get, set
    member val Type  : byte [] = null with get, set
    member val Dependencies : byte [] = null with get, set
    member val AdditionalResources : byte [] = null with get, set
    member val ETag : string = null with get, set

    new () = new CloudProcessRecord(null)

    // TODO: is this needed?
    //member this.CloneDefault() = new CloudProcessRecord(ETag = this.ETag)

    override this.ToString() = sprintf "task:%A" taskId

    static member DefaultPartitionKey = "task"

    static member CreateNew(taskId : string, info : CloudProcessInfo) =
        let serializer = ProcessConfiguration.BinarySerializer

        let record = new CloudProcessRecord(taskId)
        record.Completed      <- nullable false
        record.StartTime      <- nullableDefault
        record.CompletionTime <- nullableDefault
        record.Dependencies   <- serializer.Pickle info.Dependencies
        record.EnqueuedTime   <- nullable DateTimeOffset.Now
        record.Name     <- match info.Name with Some n -> n | None -> null
        record.Status   <- nullable(int CloudProcessStatus.Created)
        record.Type     <- info.ReturnType.Bytes
        record.TypeName <- info.ReturnTypeName
        record.CancellationTokenSource <- serializer.Pickle info.CancellationTokenSource
        info.AdditionalResources |> Option.iter (fun r -> record.AdditionalResources <- serializer.Pickle r)
        record

    member record.ToCloudProcessInfo() =
        let serializer = ProcessConfiguration.BinarySerializer
        {
            Name = match record.Name with null -> None | name -> Some name
            CancellationTokenSource = serializer.UnPickle record.CancellationTokenSource
            Dependencies = serializer.UnPickle record.Dependencies
            AdditionalResources = 
                match record.AdditionalResources with 
                | null  -> None 
                | bytes -> Some <| serializer.UnPickle bytes
            ReturnTypeName = record.TypeName
            ReturnType = new Pickle<_>(record.Type)
        }

    static member GetProcessRecord(clusterId : ClusterId, processId : string) = async {
        return new CloudProcessRecord()
        //return! Table.read<CloudProcessRecord> clusterId.StorageAccount clusterId.RuntimeTable CloudProcessRecord.DefaultPartitionKey taskId
    }

    interface IDynamoDBDocument with
        member this.ToDynamoDBDocument () =
            let doc = new Document()

            doc.["Id"]   <- Document.op_Implicit this.Id
            doc.["Name"] <- Document.op_Implicit this.Name
            doc.["ETag"] <- Document.op_Implicit this.ETag
            doc.["ResultUri"] <- Document.op_Implicit this.ResultUri
            doc.["TypeName"]  <- Document.op_Implicit this.TypeName
            doc.["Type"]      <- Document.op_Implicit this.Type
            doc.["Dependencies"] <- Document.op_Implicit this.Dependencies
            doc.["AdditionalResources"]     <- Document.op_Implicit this.AdditionalResources
            doc.["CancellationTokenSource"] <- Document.op_Implicit this.CancellationTokenSource

            this.Status    |> doIfNotNull (fun x -> doc.["Status"] <- DynamoDBEntry.op_Implicit x)
            this.Completed |> doIfNotNull (fun x -> doc.["Completed"] <- DynamoDBEntry.op_Implicit x)
            this.EnqueuedTime   |> doIfNotNull (fun x -> doc.["EnqueuedTime"] <- DynamoDBEntry.op_Implicit x)
            this.DequeuedTime   |> doIfNotNull (fun x -> doc.["DequeuedTime"] <- DynamoDBEntry.op_Implicit x)
            this.StartTime      |> doIfNotNull (fun x -> doc.["StartTime"] <- DynamoDBEntry.op_Implicit x)
            this.CompletionTime |> doIfNotNull (fun x -> doc.["CompletionTime"] <- DynamoDBEntry.op_Implicit x)

            doc

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
            if record.ResultUri <> null then return ()
            else
                do! Async.Sleep 200
                return! (this :> ICloudProcessEntry).WaitAsync()
        }
        
        member this.IncrementCompletedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementFaultedWorkItemCount(): Async<unit> = async { return () }
        member this.IncrementWorkItemCount(): Async<unit> = async { return () }
        
        member this.DeclareStatus(status: CloudProcessStatus): Async<unit> = async {
            let record = new CloudProcessRecord(processId)
            record.Status <- nullable(int status)
            record.ETag   <- "*"
            let now = nullable DateTimeOffset.Now
            match status with
            | CloudProcessStatus.Created -> 
                record.Completed    <- nullable false
                record.EnqueuedTime <- now
            | CloudProcessStatus.WaitingToRun -> 
                record.Completed    <- nullable false
                record.DequeuedTime <- now
            | CloudProcessStatus.Running -> 
                record.Completed    <- nullable false
                record.StartTime    <- now
            | CloudProcessStatus.Faulted
            | CloudProcessStatus.Completed
            | CloudProcessStatus.UserException
            | CloudProcessStatus.Canceled -> 
                record.Completed      <- nullable true
                record.CompletionTime <- nullable DateTimeOffset.Now

            | _ -> invalidArg "status" "invalid Cloud process status."

            do! Table.update clusterId.DynamoDBAccount clusterId.RuntimeTable record
        }
        
        member this.GetState(): Async<CloudProcessState> = async {
            let! jobs = 
                Table.query<WorkItemRecord> clusterId.DynamoDBAccount clusterId.RuntimeTable processId
                |> Async.StartChild
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, processId)

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

        member this.TryGetResult(): Async<CloudProcessResult option> = failwith "not implemented yet"
             
        (*async {
            let! record = CloudProcessRecord.GetProcessRecord(clusterId, processId)
            match record.ResultUri with
            | null -> return None
            | uri ->
                let! result = BlobPersist.ReadPersistedClosure<CloudProcessResult>(clusterId, uri)
                return Some result
        }*)

        member this.TrySetResult(result: CloudProcessResult, _workerId : IWorkerId): Async<bool> = 
            failwith "not implemented yet"
        (*async {
            let record = new CloudProcessRecord(taskId)
            let blobUri = guid()
            do! BlobPersist.PersistClosure(clusterId, result, blobUri, allowNewSifts = false)
            record.ResultUri <- blobUri
            record.ETag <- "*"
            let! _record = Table.merge clusterId.StorageAccount clusterId.RuntimeTable record
            return true
        }*)