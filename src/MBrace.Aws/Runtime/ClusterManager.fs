namespace MBrace.AWS.Runtime

open System
open System.Reflection

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.AWS
open MBrace.AWS.Store

/// The ClusterManager contains all resources necessary for running
/// MBrace.AWS cluster operations for the current process.
[<AutoSerializable(false)>]
type ClusterManager =
    {
        ClusterId               : ClusterId
        Configuration           : Configuration
        Serializer              : FsPicklerSerializer
        Logger                  : ISystemLogger
        Resources               : ResourceRegistry
        WorkerManager           : WorkerManager
        WorkQueue               : WorkItemQueue
        ProcessManager          : CloudProcessManager
        AssemblyManager         : StoreAssemblyManager
        LocalLoggerManager      : ILocalSystemLogManager
//        SystemLoggerManager     : TableSystemLogManager
//        CloudLoggerManager      : TableCloudLogManager
//        CancellationFactory     : TableCancellationTokenFactory
        CounterFactory          : TableCounterFactory
//        ResultAggregatorFactory : TableResultAggregatorFactory
    }
    // TODO