namespace MBrace.Aws.Runtime

open System
open System.Threading

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils

/// TopicMonitor implements an agent which periodically checks all service bus topic subscriptions
/// for messages assigned to inactive workers. If found, it will push the messages back to the main
/// Queue, to be further processed by a different worker for fault handling.
[<Sealed; AutoSerializable(false)>]
type TopicMonitor private 
        (workerManager : WorkerManager, 
         topic  : Topic, 
         queue  : Queue, 
         logger : ISystemLogger) =
    let random =
        let seed = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj())
        new Random(seed)

    // keeps a rough track of the current active cluster size
    let clusterSize = 
        let getter = async { 
            try 
                let! ws = workerManager.GetAvailableWorkers()
                return ws.Length 
            with _ -> return 2 
        }

        CacheAtom.Create(getter, intervalMilliseconds = 10000)

    let cleanupWorkerQueue (worker : IWorkerId) = async {
        let subscription = topic.GetSubscription(worker)
        let! allMessages = subscription.DequeueAllMessagesBatch()
        if not <| Array.isEmpty allMessages then
            logger.Logf LogLevel.Info "TopicMonitor : perfoming worker queue maintance for %A." worker.Id
            logger.Logf LogLevel.Info "TopicMonitor : moving %d messages to main queue for %A" allMessages.Length worker.Id
            do! queue.EnqueueMessagesBatch(allMessages)
            do! topic.Delete worker
    }

    // WorkItem queue maintenance : periodically check for non-responsive workers and cleanup their queue
    let rec loop () = async {
        do! Async.Sleep 10000
        if random.Next(0, min clusterSize.Value 4) = 0 then return! loop() else

        logger.LogInfo "TopicMonitor : starting topic maintenance."

        let! result = Async.Catch <| async {
            let! workersToCheck = workerManager.GetInactiveWorkers()
            do! workersToCheck 
                |> Seq.map (fun w -> cleanupWorkerQueue w.Id) 
                |> Async.Parallel 
                |> Async.Ignore
        }

        match result with
        | Choice1Of2 () -> 
            logger.LogInfo "TopicMonitor : maintenance complete."
        | Choice2Of2 ex -> 
            logger.Logf LogLevel.Error "TopicMonitor : maintenance error:  %A" ex

        return! loop ()
    }

    let cts = new CancellationTokenSource()
    do Async.Start(loop(), cts.Token)

    interface IDisposable with
        member __.Dispose() = cts.Cancel()

    static member Create
            (clusterId     : ClusterId, 
             workerManager : WorkerManager, 
             logger        : ISystemLogger) = async {
        let! queueT = Queue.Create(clusterId, logger) |> Async.StartChild
        let topic   = Topic.Create(clusterId, logger)
        let! queue  = queueT
        return new TopicMonitor(workerManager, topic, queue, logger)
    }