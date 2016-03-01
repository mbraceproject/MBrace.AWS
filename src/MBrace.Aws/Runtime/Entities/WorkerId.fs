namespace MBrace.AWS.Runtime

open System

open MBrace.Runtime
open MBrace.Runtime.Utils.PerformanceMonitor
open MBrace.AWS.Runtime.Utilities

open FSharp.DynamoDB

[<AutoSerializable(true)>]
type WorkerId internal (workerId : string) = 
    member this.Id = workerId

    interface IWorkerId with
        member this.CompareTo(obj: obj): int =
            match obj with
            | :? WorkerId as w -> compare workerId w.Id
            | _ -> invalidArg "obj" "invalid comparand."
        
        member this.Id: string = this.Id

    override this.ToString() = this.Id
    override this.Equals(other:obj) =
        match other with
        | :? WorkerId as w -> workerId = w.Id
        | _ -> false

    override this.GetHashCode() = hash workerId

[<ConstantHashKey("HashKey", "Worker")>]
type WorkerRecord =
    {
        [<RangeKey; CustomName("RangeKey")>]
        WorkerId : string

        Hostname : string
        ProcessId : int
        ProcessName : string
        InitializationTime : DateTimeOffset
        LastHeartBeat : DateTimeOffset
        MaxWorkItems : int
        ActiveWorkItems : int
        ProcessorCount : int
        HeartbeatInterval : TimeSpan
        HeartbeatThreshold : TimeSpan
        Version : string
        PerformanceInfo : PerformanceInfo option

        [<FsPicklerJson>]
        ExecutionStatus : WorkerExecutionStatus
    }

[<AutoOpen>]
module internal WorkerRecordUtils =
    
    let private template = template<WorkerRecord>