namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Text.RegularExpressions
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.AWS
open MBrace.AWS.Runtime.Utilities

open FSharp.DynamoDB

[<AutoOpen>]
module LoggerExtensions =
    type ISystemLogger with
        member this.LogInfof fmt    = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt   = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt

[<AutoOpen>]
module private LoggerImpl =

    let systemLogPrefix = "systemlog:"
    let cloudlogPrefix = "cloudlog:"

    let mkSystemLogHashKey (loggerId : string) = 
        sprintf "%s%s" systemLogPrefix loggerId

    let mkCloudLogHashKey (procId : string) = 
        sprintf "%s%s" cloudlogPrefix procId

    let mkRangeKey (loggerUUID : Guid) (id : int64) = 
        sprintf "%s-%010d" (loggerUUID.ToString("N")) id

    let inline tryParseRangeKey 
            (uuid     : byref<string>) 
            (id       : byref<int64>) 
            (rangeKey : string) =
        ignore uuid // squash strange unused argument warning by the F# compiler
        let tokens = rangeKey.Split('-')
        match tokens with
        | [| uuid'; id' |] ->
            uuid <- uuid'
            Int64.TryParse(id', &id)
        | _ -> false

    type IDynamoLogEntry =
        abstract LogTime : DateTimeOffset
        abstract RangeKey : string

    type SystemLogRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : string

            LoggerId : string
            Message : string
            LogTime : DateTimeOffset
            LogLevel : LogLevel
        }
    with
        interface IDynamoLogEntry with
            member __.LogTime = __.LogTime
            member __.RangeKey = __.RangeKey

        /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry
        member this.ToLogEntry() =
            new SystemLogEntry(
                this.LogLevel, 
                this.Message, 
                this.LogTime, 
                this.LoggerId)

        /// <summary>
        ///     Creates a table system log record using provided info and 
        ///     MBrace.Runtime.SystemLogEntry 
        /// </summary>
        /// <param name="worker">Table partition key.</param>
        /// <param name="entry">Input log entry.</param>
        static member FromLogEntry(loggerId : string, entry : SystemLogEntry) =
            {
                HashKey = mkSystemLogHashKey loggerId
                RangeKey = guid()
                Message = entry.Message
                LogTime = entry.DateTime
                LogLevel = entry.LogLevel
                LoggerId = loggerId
            }

    type CloudLogRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : string

            Message : string
            LogTime : DateTimeOffset
            WorkerId : string
            ProcessId : string
            WorkItemId : Guid
        }
    with
        interface IDynamoLogEntry with
            member __.LogTime = __.LogTime
            member __.RangeKey = __.RangeKey

        /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry
        member this.ToLogEntry() =
            new CloudLogEntry(
                this.ProcessId, 
                this.WorkerId, 
                this.WorkItemId, 
                this.LogTime, 
                this.Message)

        /// <summary>
        ///     Creates a table cloud log record using supplied CloudProcess 
        ///     metadata and message.s
        /// </summary>
        /// <param name="workItem">Work item generating the log entry.</param>
        /// <param name="workerId">Worker identifier generating the log entry.</param>
        /// <param name="message">User log message.</param>
        static member Create(workItem : CloudWorkItem, worker : IWorkerId, message : string) =
            {
                HashKey = mkCloudLogHashKey workItem.Process.Id
                RangeKey = guid()
                Message = message
                LogTime = DateTimeOffset.Now
                WorkerId = worker.Id
                ProcessId = workItem.Process.Id
                WorkItemId = workItem.Id
            }

    [<AutoSerializable(false)>]
    type TableLoggerMessage<'Entry> =
        | Flush of AsyncReplyChannel<unit>
        | Log   of 'Entry

    /// Local agent that writes batches of log entries to table store
    [<AutoSerializable(false)>]
    type DynamoDBLogWriter<'Entry> (table : TableContext<'Entry>, timespan : TimeSpan) =
        let queue = new Queue<'Entry> ()

        let flush () = async {
            if queue.Count > 0 then
                try do! table.BatchPutItemsAsync queue |> Async.Ignore
                with e -> Console.WriteLine (sprintf "Error writing logs: %O" e)
                queue.Clear()
        }

        let rec loop
                (lastWrite : DateTime) 
                (inbox : MailboxProcessor<TableLoggerMessage<'Entry>>) = 
            async {
                let! msg = inbox.TryReceive(100)
                match msg with
                | None when DateTime.Now - lastWrite >= timespan || queue.Count >= 25 ->
                    do! flush ()
                    return! loop DateTime.Now inbox
                | Some(Flush(channel)) ->
                    do! flush ()
                    channel.Reply()
                    return! loop DateTime.Now inbox
                | Some(Log(log)) ->
                    queue.Enqueue log
                    return! loop lastWrite inbox
                | _ ->
                    return! loop lastWrite inbox
            }

        let cts   = new CancellationTokenSource()
        let agent = MailboxProcessor.Start(
                        loop DateTime.Now, 
                        cancellationToken = cts.Token)

        /// Appends a new entry to the write queue.
        member __.LogEntry(entry : 'Entry) = agent.Post (Log entry)

        interface IDisposable with
            member __.Dispose () = 
                agent.PostAndReply Flush
                cts.Cancel ()

        /// <summary>
        ///     Creates a local log writer instance with timespan, and 
        ///     log threshold parameters
        /// </summary>
        /// <param name="tableName">Cloud table to persist logs.</param>
        /// <param name="timespan">
        ///     Timespan after which any log should be persisted.
        /// </param>
        static member Create(table : TableContext<'Entry>, ?timespan : TimeSpan) = async {
            let timespan = defaultArg timespan (TimeSpan.FromMilliseconds 500.)
            do! table.VerifyTableAsync(createIfNotExists = true)
            return new DynamoDBLogWriter<'Entry>(table, timespan)
        }


    /// Defines a local polling agent for subscribing table log events
    [<AutoSerializable(false)>]
    type DynamoDBLogPoller<'Entry when 'Entry :> IDynamoLogEntry> private (fetch : DateTimeOffset option -> Async<'Entry[]>, interval : TimeSpan) =
        let event = new Event<'Entry> ()
        let loggerInfo = new Dictionary<string, int64> ()
        let isNewLogEntry (e : 'Entry) =
            let mutable uuid = null
            let mutable id = 0L
            if tryParseRangeKey &uuid &id e.RangeKey then
                let mutable lastId = 0L
                let ok = loggerInfo.TryGetValue(uuid, &lastId)
                if ok && id <= lastId then false
                else
                    loggerInfo.[uuid] <- id
                    true
            else
                false

        let rec pollLoop (threshold : DateTimeOffset option) = async {
            do! Async.Sleep (int interval.TotalMilliseconds)
            let! logs = fetch threshold |> Async.Catch

            match logs with
            | Choice2Of2 _ -> 
                do! Async.Sleep (3 * int interval.TotalMilliseconds)
                return! pollLoop threshold

            | Choice1Of2 logs ->
                let mutable isEmpty = true
                let mutable minDate = DateTimeOffset()
                do 
                    for l in logs |> Seq.sortBy (fun l -> l.LogTime, l.RangeKey) |> Seq.filter isNewLogEntry do
                        isEmpty <- false
                        try event.Trigger l with _ -> ()
                        if minDate < l.LogTime then minDate <- l.LogTime

                if isEmpty then
                    return! pollLoop threshold
                else
                    let threshold = minDate - interval - interval - interval - interval
                    return! pollLoop (Some threshold)
        }

        let cts = new CancellationTokenSource()
        let _ = Async.StartAsTask(pollLoop None, cancellationToken = cts.Token)

        [<CLIEvent>]
        member __.Publish = event.Publish

        interface IDisposable with
            member __.Dispose() = cts.Cancel()

        static member Create(fetch : DateTimeOffset option -> Async<'Entry[]>, ?interval) =
            let interval = defaultArg interval (TimeSpan.FromMilliseconds 500.)
            new DynamoDBLogPoller<'Entry>(fetch, interval)


/// Management object for table storage based log files
[<AutoSerializable(false)>]
type DynamoDBSystemLogManager (clusterId : ClusterId) =
    static let template = template<SystemLogRecord>

    static let loggerQueryCondition =
        template.PrecomputeConditionalExpr <@ fun hk (r:SystemLogRecord) -> r.HashKey = hk @>

    static let firstLogDateFilterCondition =
        template.PrecomputeConditionalExpr <@ fun date (r:SystemLogRecord) -> date <= r.LogTime @>

    static let lastLogDateFilterCondition =
        template.PrecomputeConditionalExpr <@ fun date (r:SystemLogRecord) -> r.LogTime <= date @>

    static let betweenLogDatesFilterCondition =
        template.PrecomputeConditionalExpr <@ fun l u (r:SystemLogRecord) -> BETWEEN r.LogTime l u @>
    
    let table = clusterId.GetRuntimeLogsTable<SystemLogRecord>()

    /// <summary>
    ///     Creates a local log writer using provided logger id.
    /// </summary>
    /// <param name="loggerId">Logger identifier.</param>
    member __.CreateLogWriter(loggerId : string) = async {
        let! writer = DynamoDBLogWriter<SystemLogRecord>.Create(table)
        return {
            new IRemoteSystemLogger with
                member __.LogEntry(e : SystemLogEntry) =
                    let record = SystemLogRecord.FromLogEntry(loggerId, e)
                    writer.LogEntry record

                member __.Dispose() =
                    Disposable.dispose writer
        }
    }
        
    /// <summary>
    ///     Fetches logs matching specified constraints from table storage.
    /// </summary>
    /// <param name="loggerId">Constrain to specific logger identifier.</param>
    /// <param name="fromDate">Log entries start date.</param>
    /// <param name="toDate">Log entries finish date.</param>
    member private __.GetLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<SystemLogRecord[]> = async {
        let filterCondition =
            match fromDate, toDate with
            | None, None -> None
            | Some l, None -> firstLogDateFilterCondition l |> Some
            | None, Some u -> lastLogDateFilterCondition u |> Some
            | Some l, Some u -> betweenLogDatesFilterCondition l u |> Some

        match loggerId with
        | Some id ->
            let hkey = mkSystemLogHashKey id
            return! table.QueryAsync(loggerQueryCondition hkey, ?filterCondition = filterCondition)

        | None ->
            return! table.ScanAsync(?filterCondition = filterCondition)
    }

    /// <summary>
    ///     Asynchronously clears all system logs from table store.
    /// </summary>
    /// <param name="loggerId">Constraing to specified logger id.</param>
    member __.ClearLogs(?loggerId : string) = async {
        let! entries = async {
            match loggerId with
            | None -> return! table.ScanAsync()
            | Some id -> 
                let hkey = mkSystemLogHashKey id
                return! table.QueryAsync(loggerQueryCondition hkey)
        }

        return!
            entries
            |> Seq.map table.Template.ExtractKey
            |> Seq.chunksOf 25
            |> Seq.map table.BatchDeleteItemsAsync
            |> Async.Parallel
            |> Async.Ignore
    }

    /// <summary>
    ///     Gets a log entry observable that asynchronously polls for new logs.
    /// </summary>
    /// <param name="loggerId">Generating logger id constraint.</param>
    member this.GetSystemLogPoller (?loggerId : string) : ILogPoller<SystemLogEntry> =
        let getLogs lastDate = this.GetLogs(?loggerId = loggerId, ?fromDate = lastDate)
        let poller = DynamoDBLogPoller<SystemLogRecord>.Create(getLogs)
        let mappedEvent = poller.Publish |> Event.map (fun r -> r.ToLogEntry())

        { new ILogPoller<SystemLogEntry> with
            member x.AddHandler(handler: Handler<SystemLogEntry>): unit = 
                mappedEvent.AddHandler handler
              
            member x.Dispose(): unit = Disposable.dispose poller
              
            member x.RemoveHandler(handler: Handler<SystemLogEntry>): unit = 
                mappedEvent.RemoveHandler handler
              
            member x.Subscribe(observer: IObserver<SystemLogEntry>): IDisposable = 
                mappedEvent.Subscribe observer
        }

    interface IRuntimeSystemLogManager with 
        member x.CreateLogWriter(id: IWorkerId): Async<IRemoteSystemLogger> = async {
            return! x.CreateLogWriter(id.Id)
        }
               
        member x.GetRuntimeLogs(): Async<seq<SystemLogEntry>> = async {
            let! records = x.GetLogs()
            return records |> Seq.map (fun r -> r.ToLogEntry()) |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member x.GetWorkerLogs(id: IWorkerId): Async<seq<SystemLogEntry>> = async {
            let! records = x.GetLogs(loggerId = id.Id)
            return records |> Seq.map (fun r -> r.ToLogEntry()) |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member x.CreateLogPoller(): Async<ILogPoller<SystemLogEntry>> = async {
            return x.GetSystemLogPoller()
        }
        
        member x.CreateWorkerLogPoller(id: IWorkerId): Async<ILogPoller<SystemLogEntry>> = async {
            return x.GetSystemLogPoller(loggerId = id.Id)
        }

        member x.ClearLogs(): Async<unit> = async {
            return! x.ClearLogs()
        }
        
        member x.ClearLogs(workerId: IWorkerId): Async<unit> = async {
            return! x.ClearLogs(loggerId = workerId.Id)
        }

/// Management object for writing cloud process logs to the table store
[<AutoSerializable(false)>]
type DynamoDBCloudLogManager (clusterId : ClusterId) =
    static let template = template<CloudLogRecord>

    static let loggerQueryCondition =
        template.PrecomputeConditionalExpr <@ fun hk (r:CloudLogRecord) -> r.HashKey = hk @>

    static let firstLogDateFilterCondition =
        template.PrecomputeConditionalExpr <@ fun date (r:CloudLogRecord) -> date <= r.LogTime @>

    static let lastLogDateFilterCondition =
        template.PrecomputeConditionalExpr <@ fun date (r:CloudLogRecord) -> r.LogTime <= date @>

    static let betweenLogDatesFilterCondition =
        template.PrecomputeConditionalExpr <@ fun l u (r:CloudLogRecord) -> BETWEEN r.LogTime l u @>

    let table = clusterId.GetUserDataTable<CloudLogRecord>()

    /// <summary>
    ///     Fetches all cloud process log entries satisfying given constraints.
    /// </summary>
    /// <param name="processId">Cloud process identifier.</param>
    /// <param name="fromDate">Start date constraint.</param>
    /// <param name="toDate">Stop date constraint.</param>
    member private this.GetLogs (processId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = async {
        let filterCondition =
            match fromDate, toDate with
            | None, None -> None
            | Some l, None -> firstLogDateFilterCondition l |> Some
            | None, Some u -> lastLogDateFilterCondition u |> Some
            | Some l, Some u -> betweenLogDatesFilterCondition l u |> Some

        let hashKey = mkCloudLogHashKey processId
        return! table.QueryAsync(loggerQueryCondition hashKey, ?filterCondition = filterCondition)
    }

    /// <summary>
    ///     Fetches a cloud process log entry observable that asynchonously polls the store for new log entries.
    /// </summary>
    /// <param name="processId">Process identifier.</param>
    member this.GetLogPoller (processId : string) : ILogPoller<CloudLogEntry> =
        let getLogs lastDate = this.GetLogs(processId, ?fromDate = lastDate)
        let poller = DynamoDBLogPoller<CloudLogRecord>.Create(getLogs)
        let mappedEvent = poller.Publish |> Event.map (fun r -> r.ToLogEntry())

        { new ILogPoller<CloudLogEntry> with
            member x.AddHandler(handler: Handler<CloudLogEntry>): unit = 
                mappedEvent.AddHandler handler
                      
            member x.Dispose(): unit = Disposable.dispose poller
                      
            member x.RemoveHandler(handler: Handler<CloudLogEntry>): unit = 
                mappedEvent.RemoveHandler handler
                      
            member x.Subscribe(observer: IObserver<CloudLogEntry>): IDisposable = 
                mappedEvent.Subscribe observer }

    interface ICloudLogManager with
        member this.CreateWorkItemLogger(worker: IWorkerId, workItem: CloudWorkItem): Async<ICloudWorkItemLogger> = async {
            let! writer = DynamoDBLogWriter<CloudLogRecord>.Create(table)
            return {
                new ICloudWorkItemLogger with
                    member __.Log(message : string) =
                        let record = CloudLogRecord.Create(workItem, worker, message)
                        writer.LogEntry record

                    member __.Dispose() =
                        Disposable.dispose writer
            }
        }
        
        member this.GetAllCloudLogsByProcess(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! records = this.GetLogs(taskId)
            return records |> Seq.map (fun r -> r.ToLogEntry()) |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member this.GetCloudLogPollerByProcess(taskId: string): Async<ILogPoller<CloudLogEntry>> = async {
            return this.GetLogPoller(taskId)
        }