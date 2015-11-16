namespace MBrace.Aws.Runtime

open System

open Microsoft.FSharp.Linq.NullableOperators

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

[<AutoSerializable(false)>]
type WorkerManager private (clusterId : ClusterId, logger : ISystemLogger) =

    let jsonSerializer = ProcessConfiguration.JsonSerializer
    let pickle (value : 'T) = jsonSerializer.PickleToString(value)
    let unpickle value = jsonSerializer.UnPickleOfString<'T>(value)

    let mkWorkerState (record : WorkerRecord) =
        let workerInfo =
            { 
                Hostname  = record.Hostname
                ProcessId = record.ProcessId.GetValueOrDefault(-1)
                ProcessorCount   = record.ProcessorCount.GetValueOrDefault(-1)
                MaxWorkItemCount = record.MaxWorkItems.GetValueOrDefault(-1) 
                HeartbeatInterval  = record.HeartbeatInterval.Value  |> TimeSpan.FromTicks
                HeartbeatThreshold = record.HeartbeatThreshold.Value |> TimeSpan.FromTicks
            }
            
        { 
            Id   = new WorkerId(record.Id)
            Info = workerInfo
            CurrentWorkItemCount = record.ActiveWorkItems.GetValueOrDefault(-1)
            LastHeartbeat      = record.LastHeartbeat.Value
            InitializationTime = record.InitializationTime.Value
            ExecutionStatus    = unpickle record.Status
            PerformanceMetrics = record.GetCounters()
        }

    /// Gets all worker records.
    member __.GetAllWorkers() = async { 
        let! records = 
            Table.query<WorkerRecord> 
                clusterId.DynamoDBAccount
                clusterId.RuntimeTable
                WorkerRecord.DefaultPartitionKey
        let state = records |> Seq.map mkWorkerState |> Seq.toArray
        return state
    }

    /// 'Running' workers that fail to give heartbeats.
    member this.GetNonResponsiveWorkers(?heartbeatThreshold : TimeSpan) = async {
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return workers |> Array.filter (fun w -> 
            match w.ExecutionStatus with
            | WorkerExecutionStatus.Running when now - w.LastHeartbeat > defaultArg heartbeatThreshold w.Info.HeartbeatThreshold -> true
            | _ -> false)
    }

    /// Workers that fail to give heartbeats.
    member this.GetInactiveWorkers() = async {
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return workers |> Array.filter (fun w -> 
            now - w.LastHeartbeat > w.Info.HeartbeatThreshold)
    }

    /// Get workers that are active and actively sending heartbeats
    member this.GetAvailableWorkers() = async { 
        let now = DateTimeOffset.Now
        let! workers = this.GetAllWorkers()
        return workers |> Array.filter (fun w -> 
            match w.ExecutionStatus with
            | WorkerExecutionStatus.Running when now - w.LastHeartbeat <= w.Info.HeartbeatThreshold -> true
            | _ -> false)
    }

    /// Culls workers that have stopped sending heartbeats in a timespan larger than specified threshold
    member this.CullNonResponsiveWorkers(heartbeatThreshold : TimeSpan) = async {
        if heartbeatThreshold < TimeSpan.FromSeconds 5. then 
            invalidArg "heartbeatThreshold" "Must be at least 5 seconds."

        let! nonResponsiveWorkers = this.GetNonResponsiveWorkers(heartbeatThreshold)

        let level = 
            if nonResponsiveWorkers.Length > 0 
            then LogLevel.Warning 
            else LogLevel.Info
        logger.Logf level "WorkerManager : found %d non-responsive workers" nonResponsiveWorkers.Length

        let cullWorker(worker : WorkerState) = async {
            try 
                logger.Logf LogLevel.Info "WorkerManager : culling inactive worker '%O'" worker.Id.Id
                // TODO : change QueueFault to WorkerDeath
                do! (this :> IWorkerManager).DeclareWorkerStatus(worker.Id, WorkerExecutionStatus.WorkerDeath)
            with e ->
                logger.Logf LogLevel.Warning "WorkerManager : failed to change status for worker '%O':\n%O" worker.Id e
        }

        do! nonResponsiveWorkers 
            |> Seq.map cullWorker 
            |> Async.Parallel 
            |> Async.Ignore
    }

    member this.UnsubscribeWorker(workerId : IWorkerId) = async {
        logger.Logf LogLevel.Info "Unsubscribing worker %O" workerId
        return! (this :> IWorkerManager).DeclareWorkerStatus(workerId, WorkerExecutionStatus.Stopped)
    }

    interface IWorkerManager with
        member this.DeclareWorkerStatus
                (workerId : IWorkerId, 
                 status   : WorkerExecutionStatus) = async {
            logger.Logf LogLevel.Info "Changing worker %O status to %A" workerId status

            let record = new WorkerRecord(workerId.Id)
            record.ETag <- "*"
            record.Status <- pickle status

            do! Table.put 
                    clusterId.DynamoDBAccount
                    clusterId.RuntimeTable
                    record
        }
        
        member this.IncrementWorkItemCount(workerId: IWorkerId) = async {
            do! Table.transact2<WorkerRecord> 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    WorkerRecord.DefaultPartitionKey 
                    workerId.Id 
                    (fun e -> 
                        let ec = e.CloneDefault()
                        ec.ActiveWorkItems <- e.ActiveWorkItems ?+ 1
                        ec)
                |> Async.Ignore
        }

        member this.DecrementWorkItemCount(workerId: IWorkerId): Async<unit> = async {
            do! Table.transact2<WorkerRecord> 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    WorkerRecord.DefaultPartitionKey 
                    workerId.Id 
                    (fun e -> 
                        let ec = e.CloneDefault()
                        ec.ActiveWorkItems <- e.ActiveWorkItems ?- 1
                        ec)
                |> Async.Ignore
        }
        
        member this.GetAvailableWorkers() = this.GetAvailableWorkers()
        
        member this.SubmitPerformanceMetrics
                (workerId : IWorkerId, 
                 perf     : Utils.PerformanceMonitor.PerformanceInfo) = async {
            let record = new WorkerRecord(workerId.Id)
            record.ETag <- "*"
            record.UpdateCounters(perf)

            do! Table.put 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    record
        }
        
        member this.SubscribeWorker
                (workerId : IWorkerId, 
                 info     : WorkerInfo) = async {
            logger.Logf LogLevel.Info "Subscribing worker %O" clusterId

            let joined = DateTimeOffset.UtcNow
            let record = new WorkerRecord(workerId.Id)
            record.Hostname    <- info.Hostname
            record.ProcessName <- Diagnostics.Process.GetCurrentProcess().ProcessName
            record.ProcessId   <- nullable info.ProcessId
            record.Status  <- pickle WorkerExecutionStatus.Running
            record.Version <- ProcessConfiguration.Version.ToString(4)

            record.InitializationTime <- nullable joined
            record.LastHeartbeat      <- nullable joined

            record.ActiveWorkItems    <- nullable 0
            record.MaxWorkItems       <- nullable info.MaxWorkItemCount
            record.ProcessorCount     <- nullable info.ProcessorCount
            record.HeartbeatInterval  <- nullable info.HeartbeatInterval.Ticks
            record.HeartbeatThreshold <- nullable info.HeartbeatThreshold.Ticks

            do! Table.put
                    clusterId.DynamoDBAccount
                    clusterId.RuntimeTable 
                    record //Worker might restart but keep id

            let unsubscriber =
                { 
                    new IDisposable with
                        member x.Dispose() = 
                            this.UnsubscribeWorker(workerId)
                            |> Async.RunSync
                }
            return unsubscriber
        }
        
        member this.TryGetWorkerState(workerId: IWorkerId) = async {
            let! record = 
                Table.read<WorkerRecord> 
                    clusterId.DynamoDBAccount
                    clusterId.RuntimeTable 
                    WorkerRecord.DefaultPartitionKey
                    workerId.Id
            
            match record with
            | null -> return None
            | _    -> return Some(mkWorkerState record)
        }

    static member Create(clusterId : ClusterId, logger : ISystemLogger) =
        new WorkerManager(clusterId, logger)