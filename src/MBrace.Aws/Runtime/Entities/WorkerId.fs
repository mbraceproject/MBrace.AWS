namespace MBrace.AWS.Runtime

open System

open MBrace.Runtime
open MBrace.Runtime.Utils.PerformanceMonitor
open MBrace.AWS.Runtime.Utilities

open Amazon.DynamoDBv2.DocumentModel

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

[<AllowNullLiteral>]
type WorkerRecord(workerId : string) =
    inherit DynamoDBTableEntity(WorkerRecord.DefaultHashKey, workerId)
    
    member val Id                 = workerId with get, set
    member val Hostname           = Unchecked.defaultof<string> with get, set
    member val ProcessId          = Nullable<int>() with get, set
    member val ProcessName        = Unchecked.defaultof<string> with get, set
    member val InitializationTime = Nullable<DateTimeOffset>() with get, set
    member val LastHeartbeat      = Nullable<DateTimeOffset>() with get, set
    member val MaxWorkItems       = Nullable<int>()   with get, set
    member val ActiveWorkItems    = Nullable<int>()   with get, set
    member val ProcessorCount     = Nullable<int>()   with get, set
    member val MaxClockSpeed      = Nullable<double>() with get, set
    member val CPU                = Nullable<double>() with get, set
    member val TotalMemory        = Nullable<double>() with get, set
    member val Memory             = Nullable<double>() with get, set
    member val NetworkUp          = Nullable<double>() with get, set
    member val NetworkDown        = Nullable<double>() with get, set
    member val HeartbeatInterval  = Nullable<int64>() with get, set
    member val HeartbeatThreshold = Nullable<int64>() with get, set
    member val Version            = Unchecked.defaultof<string> with get, set
    member val Status             = Unchecked.defaultof<string> with get, set
    member val ETag               = Unchecked.defaultof<string> with get, set

    member this.GetCounters () : PerformanceInfo =
        { 
            CpuUsage         = this.CPU
            MaxClockSpeed    = this.MaxClockSpeed 
            TotalMemory      = this.TotalMemory
            MemoryUsage      = this.Memory
            NetworkUsageUp   = this.NetworkUp
            NetworkUsageDown = this.NetworkDown
        }

    member this.UpdateCounters(counters : PerformanceInfo) =
            this.CPU           <- counters.CpuUsage
            this.TotalMemory   <- counters.TotalMemory
            this.Memory        <- counters.MemoryUsage
            this.NetworkUp     <- counters.NetworkUsageUp
            this.NetworkDown   <- counters.NetworkUsageDown
            this.MaxClockSpeed <- counters.MaxClockSpeed
            this.LastHeartbeat <- nullable DateTimeOffset.Now

    member this.CloneDefault() =
        let p = new WorkerRecord(workerId)
        p.ETag <- this.ETag
        p

    override this.ToString () = sprintf "worker:%A" this.Id

    static member DefaultHashKey = "worker"

    static member FromDynamoDBDocument (doc : Document) = 
        let workerId = doc.["Id"].AsString()
        let record   = new WorkerRecord(workerId)

        record.Hostname    <- Table.readStringOrDefault doc "Hostname"
        record.ProcessName <- Table.readStringOrDefault doc "ProcessName"
        record.ETag        <- Table.readStringOrDefault doc "ETag"
        record.Version     <- Table.readStringOrDefault doc "Version"
        record.Status      <- Table.readStringOrDefault doc "Status"

        record.ProcessId       <- Table.readIntOrDefault doc "ProcessId"
        record.MaxWorkItems    <- Table.readIntOrDefault doc "MaxWorkItems"
        record.ActiveWorkItems <- Table.readIntOrDefault doc "ActiveWorkItems"
        record.ProcessorCount  <- Table.readIntOrDefault doc "ProcessorCount"
        
        record.HeartbeatInterval  <- Table.readInt64OrDefault doc "HeartbeatInterval"
        record.HeartbeatThreshold <- Table.readInt64OrDefault doc "HeartbeatThreshold"
        
        record.MaxClockSpeed <- Table.readDoubleOrDefault doc "MaxClockSpeed"
        record.CPU           <- Table.readDoubleOrDefault doc "CPU"
        record.TotalMemory   <- Table.readDoubleOrDefault doc "TotalMemory"
        record.Memory        <- Table.readDoubleOrDefault doc "Memory"
        record.NetworkUp     <- Table.readDoubleOrDefault doc "NetworkUp"
        record.NetworkDown   <- Table.readDoubleOrDefault doc "NetworkDown"

        record.InitializationTime <- Table.readDateTimeOffsetOrDefault doc "InitializationTime"
        record.LastHeartbeat      <- Table.readDateTimeOffsetOrDefault doc "LastHeartbeat"

        record

    interface IDynamoDBDocument with
        member this.ToDynamoDBDocument () =
            let doc = new Document()

            doc.["HashKey"]  <- DynamoDBEntry.op_Implicit(this.HashKey)
            doc.["RangeKey"] <- DynamoDBEntry.op_Implicit(this.RangeKey)

            doc.["Id"]       <- DynamoDBEntry.op_Implicit(this.Id)
            doc.["HostName"] <- DynamoDBEntry.op_Implicit(this.Hostname)
            doc.["Version"]  <- DynamoDBEntry.op_Implicit(this.Version)
            doc.["Status"]   <- DynamoDBEntry.op_Implicit(this.Status)
            doc.["ETag"]     <- DynamoDBEntry.op_Implicit(this.ETag)
            doc.["ProcessName"] <- DynamoDBEntry.op_Implicit(this.ProcessName)

            this.ProcessId    |> doIfNotNull (fun x -> doc.["ProcessId"] <- DynamoDBEntry.op_Implicit x)
            this.MaxWorkItems |> doIfNotNull (fun x -> doc.["MaxWorkItems"] <- DynamoDBEntry.op_Implicit x)
            this.ActiveWorkItems |> doIfNotNull (fun x -> doc.["ActiveWorkItems"] <- DynamoDBEntry.op_Implicit x)
            this.ProcessorCount  |> doIfNotNull (fun x -> doc.["ProcessorCount"] <- DynamoDBEntry.op_Implicit x)

            this.HeartbeatInterval  |> doIfNotNull (fun x -> doc.["HeartbeatInterval"] <- DynamoDBEntry.op_Implicit x)
            this.HeartbeatThreshold |> doIfNotNull (fun x -> doc.["HeartbeatThreshold"] <- DynamoDBEntry.op_Implicit x)

            this.InitializationTime |> doIfNotNull (fun x -> doc.["InitializationTime"] <- DynamoDBEntry.op_Implicit x)
            this.LastHeartbeat |> doIfNotNull (fun x -> doc.["LastHeartbeat"] <- DynamoDBEntry.op_Implicit x)
            
            this.MaxClockSpeed |> doIfNotNull (fun x -> doc.["MaxClockSpeed"] <- DynamoDBEntry.op_Implicit x)
            this.CPU           |> doIfNotNull (fun x -> doc.["CPU"] <- DynamoDBEntry.op_Implicit x)
            this.TotalMemory   |> doIfNotNull (fun x -> doc.["TotalMemory"] <- DynamoDBEntry.op_Implicit x)
            this.Memory        |> doIfNotNull (fun x -> doc.["Memory"] <- DynamoDBEntry.op_Implicit x)
            this.NetworkUp     |> doIfNotNull (fun x -> doc.["NetworkUp"] <- DynamoDBEntry.op_Implicit x)
            this.NetworkDown   |> doIfNotNull (fun x -> doc.["NetworkDown"] <- DynamoDBEntry.op_Implicit x)

            doc