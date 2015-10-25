namespace MBrace.Aws.Runtime

open System
open System.Runtime.Serialization
open System.Threading

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

/// Implements MBrace.Runtime.IWorkItemQueue on top of Amazon SQS
[<AutoSerializable(false); Sealed>]  
type WorkItemQueue private (queue : Queue, topic : Topic) =
    
    interface ICloudWorkItemQueue with
        member this.TryDequeue(id: IWorkerId): Async<ICloudWorkItemLeaseToken option> =
            failwith ""

        member this.BatchEnqueue(jobs: CloudWorkItem []): Async<unit> = 
            failwith ""

        member this.Enqueue(workItem: CloudWorkItem, isClientSideEnqueue : bool): Async<unit> =
            failwith ""

    static member Create(clusterId : ClusterId, logger : ISystemLogger) =
        failwith ""