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
type TopicMonitor private (workerManager : WorkerManager, topic : Topic, queue : Queue, logger : ISystemLogger) =
    let random =
        let seed = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj())
        new Random(seed)

    // TODO