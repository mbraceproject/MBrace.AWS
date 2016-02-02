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

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.DynamoDBv2.DocumentModel

module private Logger =
    let mkSystemLogHashKey (loggerId : string) = 
        sprintf "systemlog:%s" loggerId

    let mkCloudLogHashKey (procId : string) = 
        sprintf "cloudlog:%s" procId

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

[<AutoOpen>]
module LoggerExtensions =
    type ISystemLogger with
        member this.LogInfof fmt    = Printf.ksprintf (fun s -> this.LogInfo s) fmt
        member this.LogErrorf fmt   = Printf.ksprintf (fun s -> this.LogError s) fmt
        member this.LogWarningf fmt = Printf.ksprintf (fun s -> this.LogWarning s) fmt

/// System log record that has to eventually be stored in DynamoDB
type SystemLogRecord
        (hashKey  : string, 
         rangeKey : string, 
         message  : string, 
         logTime  : DateTimeOffset, 
         level    : int, 
         loggerId : string) =
    inherit DynamoDBTableEntity(hashKey, rangeKey)
    
    member __.Level    = level
    member __.Message  = message
    member __.LogTime  = logTime
    member __.LoggerId = loggerId

    interface IDynamoDBDocument with
        member __.ToDynamoDBDocument() =
            let doc = new Document()
            doc.[HashKey] <- DynamoDBEntry.op_Implicit hashKey
            doc.[RangeKey] <- DynamoDBEntry.op_Implicit rangeKey
            doc.["Message"] <- DynamoDBEntry.op_Implicit message
            doc.["LogTime"] <- DynamoDBEntry.op_Implicit logTime
            doc.["LogLevel"] <- DynamoDBEntry.op_Implicit level
            doc.["LoggerId"] <- DynamoDBEntry.op_Implicit loggerId

            doc

    static member FromDynamoDBDocument(doc : Document) =
            let hashKey = doc.[HashKey].AsString()
            let rangeKey = doc.[RangeKey].AsString()
            let message = doc.["Message"].AsString()
            let logTime = doc.["LogTime"].AsDateTimeOffset()
            let logLevel = doc.["LogLevel"].AsInt()
            let loggerId = doc.["LoggerId"].AsString()
            new SystemLogRecord(hashKey, rangeKey, message, logTime, logLevel, loggerId)

    /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry
    member this.ToLogEntry() =
        new SystemLogEntry(
            enum this.Level, 
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
        new SystemLogRecord(
            Logger.mkSystemLogHashKey loggerId, 
            null, 
            entry.Message, 
            entry.DateTime, 
            int entry.LogLevel, 
            loggerId)

/// Cloud process log record
type CloudLogRecord
        (hashKey    : string, 
         rangeKey   : string, 
         message    : string, 
         logTime    : DateTimeOffset, 
         workerId   : string, 
         procId     : string, 
         workItemId : Guid) =
    inherit DynamoDBTableEntity(hashKey, rangeKey)
    
    member __.Message    = message
    member __.LogTime    = logTime
    member __.WorkerId   = workerId
    member __.ProcessId  = procId
    member __.WorkItemId = workItemId

    /// Converts LogEntry table entity to MBrace.Runtime.SystemLogEntry
    member this.ToLogEntry() =
        new CloudLogEntry(
            this.ProcessId, 
            this.WorkerId, 
            this.WorkItemId, 
            this.LogTime, 
            this.Message)

    interface IDynamoDBDocument with
        member __.ToDynamoDBDocument() =
            let doc = new Document()
            doc.[HashKey] <- DynamoDBEntry.op_Implicit hashKey
            doc.[RangeKey] <- DynamoDBEntry.op_Implicit rangeKey
            doc.["Message"] <- DynamoDBEntry.op_Implicit message
            doc.["LogTime"] <- DynamoDBEntry.op_Implicit logTime
            doc.["WorkerId"] <- DynamoDBEntry.op_Implicit workerId
            doc.["ProcId"] <- DynamoDBEntry.op_Implicit procId
            doc.["WorkItemId"] <- DynamoDBEntry.op_Implicit workItemId
            doc

    static member FromDynamoDBDocument(doc : Document) =
            let hashKey = doc.[HashKey].AsString()
            let rangeKey = doc.[RangeKey].AsString()
            let message = doc.["Message"].AsString()
            let logTime = doc.["LogTime"].AsDateTimeOffset()
            let workerId = doc.["WorkerId"].AsString()
            let loggerId = doc.["ProcId"].AsString()
            let workItemId = doc.["WorkItemId"].AsGuid()
            new CloudLogRecord(hashKey, rangeKey, message, logTime, workerId, loggerId, workItemId)

    /// <summary>
    ///     Creates a table cloud log record using supplied CloudProcess 
    ///     metadata and message.s
    /// </summary>
    /// <param name="workItem">Work item generating the log entry.</param>
    /// <param name="workerId">Worker identifier generating the log entry.</param>
    /// <param name="message">User log message.</param>
    static member Create
            (workItem : CloudWorkItem, 
             worker   : IWorkerId, 
             message  : string) =
        let hashKey = Logger.mkCloudLogHashKey workItem.Process.Id
        new CloudLogRecord(
            hashKey, 
            null, 
            message, 
            DateTimeOffset.Now, 
            worker.Id, 
            workItem.Process.Id, 
            workItem.Id)

[<AutoSerializable(false)>]
type private TableLoggerMessage<'Entry when 'Entry :> DynamoDBTableEntity> =
    | Flush of AsyncReplyChannel<unit>
    | Log   of 'Entry

/// Local agent that writes batches of log entries to table store
[<AutoSerializable(false)>]
type private DynamoDBLogWriter<'Entry when 'Entry :> DynamoDBTableEntity
                                      and  'Entry :> IDynamoDBDocument> 
        private (account      : AWSAccount,
                 tableName    : string, 
                 timespan     : TimeSpan, 
                 logThreshold : int) =
    let queue = new Queue<'Entry> ()

    let flush () = async {
        if queue.Count > 0 then
            let entries = queue |> Seq.toArray
            queue.Clear()

            do! Table.putBatch 
                    account
                    tableName 
                    entries
                |> Async.Catch
                |> Async.Ignore
    }

    let rec loop 
            (lastWrite : DateTime) 
            (inbox : MailboxProcessor<TableLoggerMessage<'Entry>>) = 
        async {
            let! msg = inbox.TryReceive(100)
            match msg with
            | None when DateTime.Now - lastWrite >= timespan || queue.Count >= logThreshold ->
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
    /// <param name="logThreshold">
    ///     Minimum number of logs to force instance flushing of log entries.
    /// </param>
    static member Create
            (account     : AWSAccount,
             tableName     : string, 
             ?timespan     : TimeSpan, 
             ?logThreshold : int) = 
        async {
            let timespan     = defaultArg timespan (TimeSpan.FromMilliseconds 500.)
            let logThreshold = defaultArg logThreshold 100
            do! Table.createIfNotExists 
                    account
                    tableName
                    None
                    None
            return new DynamoDBLogWriter<'Entry>(
                    account, tableName, timespan, logThreshold)
        }


/// Defines a local polling agent for subscribing table log events
[<AutoSerializable(false)>]
type private DynamoDBLogPoller<'Entry when 'Entry :> DynamoDBTableEntity> private (fetch : DateTimeOffset option -> Async<ICollection<'Entry>>, getLogDate : 'Entry -> DateTimeOffset, interval : TimeSpan) =
    let event = new Event<'Entry> ()
    let loggerInfo = new Dictionary<string, int64> ()
    let isNewLogEntry (e : 'Entry) =
        let mutable uuid = null
        let mutable id = 0L
        if Logger.tryParseRangeKey &uuid &id e.RangeKey then
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
                for l in logs |> Seq.sortBy (fun l -> getLogDate l, l.RangeKey) |> Seq.filter isNewLogEntry do
                    isEmpty <- false
                    try event.Trigger l with _ -> ()
                    if minDate < getLogDate l then minDate <- getLogDate l

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

    static member Create(fetch : DateTimeOffset option -> Async<ICollection<'Entry>>, getLogDate : 'Entry -> DateTimeOffset, ?interval) =
        let interval = defaultArg interval (TimeSpan.FromMilliseconds 500.)
        new DynamoDBLogPoller<'Entry>(fetch, getLogDate, interval)


/// Management object for table storage based log files
[<AutoSerializable(false)>]
type DynamoDBSystemLogManager (clusterId : ClusterId) =
    let table = Table.LoadTable(clusterId.DynamoDBAccount.DynamoDBClient, clusterId.RuntimeLogsTable)

    /// <summary>
    ///     Creates a local log writer using provided logger id.
    /// </summary>
    /// <param name="loggerId">Logger identifier.</param>
    member __.CreateLogWriter(loggerId : string) = async {
        let! writer = DynamoDBLogWriter<SystemLogRecord>.Create(clusterId.DynamoDBAccount, clusterId.RuntimeLogsTable)
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
    member __.GetLogs(?loggerId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<ICollection<SystemLogRecord>> = async {
        let mkRequest() =
            let queryRequest = QueryRequest(TableName = clusterId.RuntimeLogsTable)

            loggerId |> Option.iter (fun id ->
                let eqCond = new Condition()
                eqCond.ComparisonOperator <- ComparisonOperator.EQ
                eqCond.AttributeValueList.Add(new AttributeValue(Logger.mkSystemLogHashKey id))
                queryRequest.KeyConditions.Add(HashKey, eqCond))

            match fromDate, toDate with
            | None, None -> ()
            | Some fd, None ->
                let cond = new Condition()
                cond.ComparisonOperator <- ComparisonOperator.GE
                cond.AttributeValueList.Add(new AttributeValue(fd.ToISO8601String()))
                queryRequest.QueryFilter.["LogTime"] <- cond

            | None, Some td ->
                let cond = new Condition()
                cond.ComparisonOperator <- ComparisonOperator.LE
                cond.AttributeValueList.Add(new AttributeValue(td.ToISO8601String()))
                queryRequest.QueryFilter.["LogTime"] <- cond

            | Some fd, Some td ->
                let cond = new Condition()
                cond.ComparisonOperator <- ComparisonOperator.BETWEEN
                cond.AttributeValueList.AddRange [|AttributeValue(fd.ToISO8601String()); AttributeValue(td.ToISO8601String())|]
                queryRequest.QueryFilter.["LogTime"] <- cond

            queryRequest

        return! Table.queryDocs clusterId.DynamoDBAccount mkRequest SystemLogRecord.FromDynamoDBDocument
    }

    /// <summary>
    ///     Asynchronously clears all system logs from table store.
    /// </summary>
    /// <param name="loggerId">Constraing to specified logger id.</param>
    member __.ClearLogs(?loggerId : string) = async {
        return failwith "not implemented"
//        let query = new TableQuery<SystemLogRecord>()
//        let query =
//            match loggerId with
//            | None -> query
//            | Some id -> query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkSystemLogPartitionKey id))
//
//        let query = query.Select [| "RangeKey" |]
//
//        do! table.CreateIfNotExistsAsyncSafe(maxRetries = 3)
//        
//        let! entries = Table.queryAsync table query
//        return!
//            entries
//            |> Seq.groupBy (fun e -> e.PartitionKey)
//            |> Seq.collect (fun (_,es) -> Seq.chunksOf 100 es)
//            |> Seq.map (fun chunk ->
//                async {
//                    let batchOp = new TableBatchOperation()
//                    do for e in chunk do batchOp.Delete e
//                    let! _result = table.ExecuteBatchAsync batchOp |> Async.AwaitTaskCorrect
//                    return ()
//                })
//            |> Async.Parallel
//            |> Async.Ignore
    }

    /// <summary>
    ///     Gets a log entry observable that asynchronously polls for new logs.
    /// </summary>
    /// <param name="loggerId">Generating logger id constraint.</param>
    member this.GetSystemLogPoller (?loggerId : string) : ILogPoller<SystemLogEntry> =
        let getLogs lastDate = this.GetLogs(?loggerId = loggerId, ?fromDate = lastDate)
        let getDate (e : SystemLogRecord) = e.LogTime
        let poller = DynamoDBLogPoller<SystemLogRecord>.Create(getLogs, getDate)
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
//    let table = clusterId.StorageAccount.GetTableReference clusterId.UserDataTable

    /// <summary>
    ///     Fetches all cloud process log entries satisfying given constraints.
    /// </summary>
    /// <param name="processId">Cloud process identifier.</param>
    /// <param name="fromDate">Start date constraint.</param>
    /// <param name="toDate">Stop date constraint.</param>
    member this.GetLogs (processId : string, ?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) : Async<ICollection<CloudLogRecord>> =
        async {
            return raise <| NotImplementedException()
//            let query = new TableQuery<CloudLogRecord>()
//            let filters = 
//                [ Some(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Logger.mkCloudLogPartitionKey processId))
//                  fromDate   |> Option.map (fun t -> TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThanOrEqual, t))
//                  toDate     |> Option.map (fun t -> TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThanOrEqual, t)) ]
//
//            let filter = 
//                filters 
//                |> List.fold (fun state filter -> 
//                    match state, filter with
//                    | None, None -> None
//                    | Some f, None 
//                    | None, Some f -> Some f
//                    | Some f1, Some f2 -> Some <| TableQuery.CombineFilters(f1, TableOperators.And, f2) ) None
//
//            let query =
//                match filter with
//                | None -> query
//                | Some f -> query.Where(f) 
//
//            do! table.CreateIfNotExistsAsyncSafe(maxRetries = 3)
//            return! Table.queryAsync table query
        }

    /// <summary>
    ///     Fetches a cloud process log entry observable that asynchonously polls the store for new log entries.
    /// </summary>
    /// <param name="processId">Process identifier.</param>
    member this.GetLogPoller (processId : string) : ILogPoller<CloudLogEntry> =
        let getLogs lastDate = this.GetLogs(processId, ?fromDate = lastDate)
        let getDate (e : CloudLogRecord) = e.LogTime
        let poller = DynamoDBLogPoller<CloudLogRecord>.Create(getLogs, getDate)
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
            return raise <| NotImplementedException()
//            let! writer = DynamoDBLogWriter<CloudLogRecord>.Create(table)
//            return {
//                new ICloudWorkItemLogger with
//                    member __.Log(message : string) =
//                        let record = CloudLogRecord.Create(workItem, worker, message)
//                        writer.LogEntry record
//
//                    member __.Dispose() =
//                        Disposable.dispose writer
//            }
        }
        
        member this.GetAllCloudLogsByProcess(taskId: string): Async<seq<CloudLogEntry>> = async {
            let! records = this.GetLogs(taskId)
            return records |> Seq.map (fun r -> r.ToLogEntry()) |> Seq.sortBy (fun e -> e.DateTime)
        }
        
        member this.GetCloudLogPollerByProcess(taskId: string): Async<ILogPoller<CloudLogEntry>> = async {
            return this.GetLogPoller(taskId)
        }