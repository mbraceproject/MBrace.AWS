namespace MBrace.Aws.Runtime

open System
open System.Reflection

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.Aws
open MBrace.Aws.Store

/// The ClusterManager contains all resources necessary for running
/// MBrace.Aws cluster operations for the current process.
[<AutoSerializable(false)>]
type ClusterManager =
    {
        ClusterId               : ClusterId
//        Configuration           : Configuration
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