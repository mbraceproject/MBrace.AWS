namespace MBrace.AWS.Runtime

open System

open FSharp.DynamoDB

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime

open MBrace.AWS
open MBrace.AWS.Runtime.Utilities

[<AutoSerializable(false)>]
type WorkerManager private (clusterId : ClusterId, logger : ISystemLogger) =

    let getKey (w:IWorkerId) = TableKey.Range(w.Id)
    let getTable() = clusterId.GetRuntimeTable<WorkerRecord>()

    let mkWorkerState (record : WorkerRecord) : WorkerState =
        let workerInfo =
            { 
                Hostname  = record.Hostname
                ProcessId = record.ProcessId
                ProcessorCount   = record.ProcessorCount
                MaxWorkItemCount = record.MaxWorkItems
                HeartbeatInterval  = record.HeartbeatInterval
                HeartbeatThreshold = record.HeartbeatThreshold
            }
            
        { 
            Id   = new WorkerId(record.WorkerId)
            Info = workerInfo
            CurrentWorkItemCount = record.ActiveWorkItems
            LastHeartbeat      = record.LastHeartBeat
            InitializationTime = record.InitializationTime
            ExecutionStatus    = record.ExecutionStatus
            PerformanceMetrics = record.PerformanceInfo
        }

    /// Gets all worker records.
    member __.GetAllWorkers() = async { 
        let! records = getTable().QueryAsync workerRecordKeyCondition
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
                logger.LogInfof "WorkerManager : culling inactive worker '%O'" worker.Id.Id
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
        logger.LogInfof "Unsubscribing worker %O" workerId
        return! (this :> IWorkerManager).DeclareWorkerStatus(workerId, WorkerExecutionStatus.Stopped)
    }

    interface IWorkerManager with
        member __.DeclareWorkerStatus(workerId : IWorkerId, status : WorkerExecutionStatus) = async {
            logger.LogInfof "Changing worker %O status to %A" workerId status

            let! _ = getTable().UpdateItemAsync(getKey workerId, updateExecutionStatus status)
            return ()
        }
        
        member __.IncrementWorkItemCount(workerId: IWorkerId) = async {
            let! _ = getTable().UpdateItemAsync(getKey workerId, incrWorkItemCount)
            return ()
        }

        member __.DecrementWorkItemCount(workerId: IWorkerId): Async<unit> = async {
            let! _ = getTable().UpdateItemAsync(getKey workerId, decrWorkItemCount)
            return ()
        }
        
        member this.GetAvailableWorkers() = this.GetAvailableWorkers()
        
        member __.SubmitPerformanceMetrics(workerId : IWorkerId, perf : Utils.PerformanceMonitor.PerformanceInfo) = async {
            let! _ = getTable().UpdateItemAsync(getKey workerId, updatePerfMetrics perf)
            return ()
        }
        
        member this.SubscribeWorker(workerId : IWorkerId, info : WorkerInfo) = async {
            logger.LogInfof "Subscribing worker %O" clusterId

            let joined = DateTimeOffset.Now
            let record =
                {
                    WorkerId = workerId.Id
                    Hostname = info.Hostname
                    ProcessName = Diagnostics.Process.GetCurrentProcess().ProcessName
                    ProcessId = info.ProcessId
                    ExecutionStatus = WorkerExecutionStatus.Running
                    Version = ProcessConfiguration.Version.ToString(4)
                    
                    InitializationTime = joined
                    LastHeartBeat = joined

                    ActiveWorkItems = 0
                    MaxWorkItems = info.MaxWorkItemCount
                    ProcessorCount = info.ProcessorCount
                    HeartbeatInterval = info.HeartbeatInterval
                    HeartbeatThreshold = info.HeartbeatThreshold
                    PerformanceInfo = Utils.PerformanceMonitor.PerformanceInfo.Empty
                }

            let! key = getTable().PutItemAsync(record)

            let rec heartbeat interval = async {
                do! Async.Sleep interval
                let now = DateTimeOffset.Now
                try
                    let! _ = getTable().UpdateItemAsync(key, updateLastHeartbeat now)
                    logger.Logf LogLevel.Debug "sending heartbeat"
                    return ()
                with e ->
                    logger.Logf LogLevel.Error "could not send heartbeat: %O" e

                return! heartbeat interval
            }

            let cts = new System.Threading.CancellationTokenSource()
            Async.Start(heartbeat (int info.HeartbeatInterval.TotalMilliseconds), cts.Token)

            let unsubscriber =
                { 
                    new IDisposable with
                        member x.Dispose() =
                            cts.Cancel()
                            this.UnsubscribeWorker(workerId)
                            |> Async.RunSync
                }
            return unsubscriber
        }
        
        member __.TryGetWorkerState(workerId: IWorkerId) = async {
            try
                let! record = getTable().GetItemAsync(getKey workerId)
                return Some(mkWorkerState record)
            with :? ResourceNotFoundException -> return None
        }

    static member Create(clusterId : ClusterId, logger : ISystemLogger) =
        new WorkerManager(clusterId, logger)