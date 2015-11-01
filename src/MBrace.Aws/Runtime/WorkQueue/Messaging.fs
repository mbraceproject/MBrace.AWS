namespace MBrace.Aws.Runtime

open System
open System.IO
open System.Threading.Tasks

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities

/// Topic client implementation
[<Sealed; AutoSerializable(false)>]
type internal Topic (clusterId : ClusterId, logger : ISystemLogger) = 
    let topic = clusterId.ServiceBusAccount.CreateTopicClient(clusterId.WorkItemTopic)

    member this.GetMessageCountAsync() = 
        failwith "not implemented yet"

    member this.GetSubscription(subscriptionId : IWorkerId) : Subscription = 
        failwith "not implemented yet"
    
    member this.EnqueueBatch(jobs : CloudWorkItem []) : Async<unit> = 
        failwith "not implemented yet"
    
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        failwith "not implemented yet"

    static member Create(config, logger : ISystemLogger) = 
        failwith "not implemented yet"

/// Queue client implementation
[<Sealed; AutoSerializable(false)>]
type internal Queue (clusterId : ClusterId, logger : ISystemLogger) = 
    let sqsClient = clusterId.SQSAccount.SQSClient
    let queueUri  = clusterId.WorkItemQueue

    member this.GetMessageCountAsync() = 
        failwith "not implemented yet"

    member this.EnqueueBatch(jobs : CloudWorkItem []) = 
        failwith "not implemented yet"
            
    member this.Enqueue(workItem : CloudWorkItem, allowNewSifts : bool) = 
        failwith "not implemented yet"
    
    member this.TryDequeue(workerId : IWorkerId) : Async<ICloudWorkItemLeaseToken option> = 
        failwith "not implemented yet"

    member this.EnqueueMessagesBatch(messages : seq<byte[]>) = 
        failwith "not implemented yet"
        
    static member Create(clusterId : ClusterId, logger : ISystemLogger) = 
        failwith "not implemented yet"