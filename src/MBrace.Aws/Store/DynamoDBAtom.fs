namespace MBrace.AWS.Store

open System
open System.Net
open System.IO
open System.Runtime.Serialization
open System.Text.RegularExpressions

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.DynamoDBv2.DataModel

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

module Internals =

    [<CLIMutable>]
    [<DynamoDBTable("AtomEntry")>]
    type AtomEntry =
        {
            [<DynamoDBHashKey>]
            HashKey : string
            [<DynamoDBProperty>]
            LastModified : DateTime
            [<DynamoDBProperty>]
            Payload : byte []
            [<DynamoDBVersion>]
            Version : Nullable<int>        
        }

open Internals

[<AutoOpen>]
module private DynamoDBAtomUtils =


    // max item size is 400KB including attribute length, etc.
    // allow 1KB for all that leaves 399KB for actual payload
    let maxPayload = 399L * 1024L

    let getRandomTableName (prefix : string) =
        sprintf "%s-%s" prefix <| Guid.NewGuid().ToString("N")

    let mkRandomTableRegex prefix = new Regex(sprintf "%s-[0-9a-z]{32}" prefix, RegexOptions.Compiled)

    let mkTag () = Guid.NewGuid().ToString()
    let mkTimeStamp () = DateTime.UtcNow.ToString(System.Globalization.CultureInfo.InvariantCulture)

    let createTableFaultPolicy =
        Policy(fun retries exn ->
            if retries < 10 && StoreException.Conflict exn then Some(TimeSpan.FromSeconds 2.)
            else None)

    let mkConditionalRetryPolicy maxRetries = 
        Policy(fun retries exn ->
            match exn with
            | :? ConditionalCheckFailedException when maxRetries |> Option.forall (fun m -> retries < m) ->
                Some(TimeSpan.FromMilliseconds 100.)
            | _ -> None)

    let infiniteConditionalRetryPolicy = mkConditionalRetryPolicy None

    let mkEntry hashKey version (newValue : 'T) =
        let blob = ProcessConfiguration.BinarySerializer.Pickle<'T>(newValue)
        { HashKey = hashKey ; LastModified = DateTime.UtcNow ; Payload = blob ; Version = version }

/// CloudAtom implementation on top of Amazon DynamoDB
[<AutoSerializable(true) ; Sealed; DataContract>]
type DynamoDBAtom<'T> internal (tableName : string, account : AwsAccount, hashKey : string) =

    [<DataMember(Name = "AWSAccount")>]
    let account = account

    [<DataMember(Name = "TableName")>]
    let tableName = tableName

    [<DataMember(Name = "HashKey")>]
    let hashKey = hashKey

    let getContext() = new DynamoDBContext(account.DynamoDBClient)
    let getCfg() = new DynamoDBOperationConfig(OverrideTableName = tableName)

    let getItemAsync () = async {
        let ctx = getContext()
        let! item = ctx.LoadAsync<AtomEntry>(hashKey, getCfg()) |> Async.AwaitTaskCorrect
        return item
    }

    let getValue (entry : AtomEntry) =
        ProcessConfiguration.BinarySerializer.UnPickle<'T>(entry.Payload)

    interface CloudAtom<'T> with
        member __.Container = tableName
        member __.Id = hashKey

        member __.GetValueAsync () = async {
            let! entry = getItemAsync()
            return getValue entry
        }

        member __.Value = (__ :> CloudAtom<'T>).GetValueAsync() |> Async.RunSync

        member __.Dispose (): Async<unit> = async {
            let ctx = getContext()
            do! ctx.DeleteAsync<AtomEntry>(hashKey, getCfg()) |> Async.AwaitTaskCorrect
        }

        member __.ForceAsync (newValue : 'T) = async {
            let! ct = Async.CancellationToken
            let ctx = getContext()
            let cfg = getCfg()
            cfg.SkipVersionCheck <- Nullable true
            let entry = mkEntry hashKey (Nullable()) newValue
            do! ctx.SaveAsync<AtomEntry>(entry, cfg, ct) |> Async.AwaitTaskCorrect
        }

        member __.TransactAsync (transaction, maxRetries) = async {
            let policy = 
                match maxRetries with
                | None -> infiniteConditionalRetryPolicy
                | Some _ -> mkConditionalRetryPolicy maxRetries

            return! retryAsync policy <| 
                async {
                    let! ct = Async.CancellationToken
                    let! oldEntry = getItemAsync()
                    let oldValue = getValue oldEntry
                    let returnValue, newValue = transaction oldValue
                    let newEntry = mkEntry hashKey oldEntry.Version newValue

                    let ctx = getContext()
                    let cfg = getCfg()
                    do! ctx.SaveAsync<AtomEntry>(newEntry, cfg, ct) |> Async.AwaitTaskCorrect

                    return returnValue
                }
        }

/// CloudAtom provider implementation on top of Amazon DynamoDB.
[<Sealed; DataContract>]
type DynamoDBAtomProvider private (account : AwsAccount, defaultTable : string, tablePrefix : string, provisionedThroughput : int64) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "DefaultTable")>]
    let defaultTable = defaultTable

    [<DataMember(Name = "TablePrefix")>]
    let tablePrefix = tablePrefix

    [<DataMember(Name = "ProvisionedThroughput")>]
    let provisionedThroughput = provisionedThroughput

    let ensureTableExists (tableName : string) =
        retryAsync createTableFaultPolicy <| async {
            let! ct = Async.CancellationToken
            let! listedTables = account.DynamoDBClient.ListTablesAsync(ct) |> Async.AwaitTaskCorrect
            if listedTables.TableNames |> Seq.exists(fun tn -> tn = tableName) |> not then
                let ctr = new CreateTableRequest(TableName = tableName)
                ctr.KeySchema.Add <| KeySchemaElement("HashKey", KeyType.HASH)
                ctr.AttributeDefinitions.Add <| AttributeDefinition("HashKey", ScalarAttributeType.S)
                ctr.ProvisionedThroughput <- new ProvisionedThroughput(provisionedThroughput, provisionedThroughput)

                let! _resp = account.DynamoDBClient.CreateTableAsync(ctr, ct) |> Async.AwaitTaskCorrect
                ()

            let rec awaitReady retries = async {
                if retries = 0 then return failwithf "Failed to create table '%s'" tableName
                let! descr = account.DynamoDBClient.DescribeTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
                if descr.Table.TableStatus <> TableStatus.ACTIVE then
                    do! Async.Sleep 1000
                    return! awaitReady (retries - 1)
            }

            do! awaitReady 20
        }

    /// <summary>
    /// Creates an AWS DynamoDB-based atom provider that
    /// connects to provided DynamoDB table.
    /// </summary>
    /// <param name="account">AWS account to be used by the provider.</param>
    /// <param name="defaultTable">Default table container.</param>
    /// <param name="provisionedThroughput">DynamoDB provision throughput. Defaults to 20.</param>
    static member Create (account : AwsAccount, ?defaultTable : string, ?tablePrefix : string, ?provisionedThroughput : int64) =
        let tablePrefix =
            match tablePrefix with
            | None -> "cloudAtom"
            | Some tp when tp.Length > 220 -> invalidArg "tablePrefix" "must be at most 220 characters long."
            | Some tp -> Validate.tableName tp ; tp

        let provisionedThroughput = defaultArg provisionedThroughput 20L

        let defaultTable = 
            match defaultTable with
            | Some x -> Validate.tableName x ; x
            | _ -> getRandomTableName tablePrefix

        new DynamoDBAtomProvider(account, defaultTable, tablePrefix, provisionedThroughput)

    /// Table prefix used in random table name generation
    member __.TablePrefix = tablePrefix

    /// <summary>
    ///     Clears all randomly named DynamoDB tables that match the given prefix.
    /// </summary>
    /// <param name="prefix">Prefix to clear. Defaults to the table prefix of the current store instance.</param>
    member this.ClearTablesAsync(?prefix : string) = async {
        let tableRegex = mkRandomTableRegex (defaultArg prefix tablePrefix)
        let store = this :> ICloudAtomProvider
        let! ct = Async.CancellationToken
        let! tables = account.DynamoDBClient.ListTablesAsync(ct) |> Async.AwaitTaskCorrect
        do! tables.TableNames
            |> Seq.filter tableRegex.IsMatch 
            |> Seq.map (fun b -> store.DisposeContainer b)
            |> Async.Parallel
            |> Async.Ignore
    }
        
    interface ICloudAtomProvider with
        member __.Id = sprintf "arn:aws:dynamodb:table/%s" defaultTable
        member __.Name = "AWS DynamoDB CloudAtom Provider"
        member __.DefaultContainer = defaultTable

        member __.WithDefaultContainer (tableName : string) =
            Validate.tableName tableName
            new DynamoDBAtomProvider(account, tableName, tablePrefix, provisionedThroughput) :> _

        member __.IsSupportedValue(value : 'T) = 
            let size = ProcessConfiguration.BinarySerializer.ComputeSize value
            size <= maxPayload

        member __.CreateAtom<'T>(tableName, atomId, initValue) = async {
            let! ct = Async.CancellationToken
            Validate.tableName tableName
            do! ensureTableExists tableName

            let ctx = new DynamoDBContext(account.DynamoDBClient)
            let cfg = new DynamoDBOperationConfig(OverrideTableName = tableName)

            let entry = mkEntry atomId (Nullable()) initValue

            do! ctx.SaveAsync<AtomEntry>(entry, cfg, ct) |> Async.AwaitTaskCorrect

            return new DynamoDBAtom<'T>(tableName, account, atomId) :> CloudAtom<'T>
        }
        
        member __.GetAtomById(tableName : string, atomId : string) = async {
            Validate.tableName tableName
            // TODO : check that table entry exists?
            return new DynamoDBAtom<'T>(tableName, account, atomId) :> CloudAtom<'T>
        }

        member __.GetRandomAtomIdentifier() = sprintf "cloudAtom-%s" <| mkUUID()
        member __.GetRandomContainerName() = getRandomTableName tablePrefix

        member __.DisposeContainer(tableName : string) = async {
            Validate.tableName tableName
            let req = DeleteTableRequest(TableName = tableName)
            do! account.DynamoDBClient.DeleteTableAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }