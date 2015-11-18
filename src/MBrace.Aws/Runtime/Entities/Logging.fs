namespace MBrace.Aws.Runtime

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
open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

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
    
    member val Level    = level with get, set 
    member val Message  = message with get, set
    member val LogTime  = logTime with get, set
    member val LoggerId = loggerId with get, set

    new () = new SystemLogRecord(null, null, null, Unchecked.defaultof<_>, -1, null)

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
    
    member val Message    = message with get, set
    member val LogTime    = logTime with get, set
    member val WorkerId   = workerId with get, set
    member val ProcessId  = procId with get, set
    member val WorkItemId = workItemId with get, set

    new () = new CloudLogRecord(null, null, null, Unchecked.defaultof<_>, null, null, Unchecked.defaultof<_>)

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
type private CloudTableLogWriter<'Entry when 'Entry :> DynamoDBTableEntity
                                        and  'Entry :> IDynamoDBDocument> 
        private (clusterId    : ClusterId,
                 tableName    : string, 
                 timespan     : TimeSpan, 
                 logThreshold : int) =
    let queue = new Queue<'Entry> ()

    let flush () = async {
        if queue.Count > 0 then
            let entries = queue |> Seq.toArray
            queue.Clear()

            do! Table.putBatch 
                    clusterId.DynamoDBAccount 
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
            (clusterId     : ClusterId,
             tableName     : string, 
             ?timespan     : TimeSpan, 
             ?logThreshold : int) = 
        async {
            let timespan     = defaultArg timespan (TimeSpan.FromMilliseconds 500.)
            let logThreshold = defaultArg logThreshold 100
            do! Table.createIfNotExists 
                    clusterId.DynamoDBAccount 
                    tableName
                    None
                    None
            return new CloudTableLogWriter<'Entry>(
                    clusterId, tableName, timespan, logThreshold)
        }