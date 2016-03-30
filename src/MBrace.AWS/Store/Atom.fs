namespace MBrace.AWS.Store

open System
open System.Net
open System.IO
open System.Runtime.Serialization
open System.Text.RegularExpressions

open Amazon.Runtime
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open FSharp.AWS.DynamoDB

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry
open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

[<AutoOpen>]
module private DynamoDBAtomUtils =

    [<ConstantRangeKey("RangeKey", "CloudAtom")>]
    type AtomEntry =
        {
            [<HashKey>]
            HashKey : string

            Data : MemoryStream

            ETag : string

            Timestamp : DateTimeOffset
        }

    let atomEntry = RecordTemplate.Define<AtomEntry> ()

    let withEtag = atomEntry.PrecomputeConditionalExpr <@ fun etag ae -> ae.ETag = etag @>

    let updateEntry = 
        atomEntry.PrecomputeUpdateExpr                             
                    <@ fun (data:MemoryStream) time etag r -> 
                            { r with 
                                Data = data
                                Timestamp = time
                                ETag = etag } @>


    // max item size is 400KB including attribute length, etc.
    // allow 1KB for all that leaves 399KB for actual payload
    let maxPayload = 399L * 1024L

    let getRandomTableName (prefix : string) =
        sprintf "%s-%s" prefix <| Guid.NewGuid().ToString("N")

    let mkRandomTableRegex prefix = new Regex(sprintf "%s-[0-9a-z]{32}" prefix, RegexOptions.Compiled)

    let serialize (value : 'T) =
        let m = new MemoryStream()
        ProcessConfiguration.BinarySerializer.Serialize(m, value, leaveOpen = true)
        m.Position <- 0L
        m

    let mkConditionalRetryPolicy maxRetries = 
        Policy(fun retries exn ->
            match exn with
            | :? ConditionalCheckFailedException when maxRetries |> Option.forall (fun m -> retries < m) ->
                Some(TimeSpan.FromMilliseconds 100.)
            | _ -> None)

    let infiniteConditionalRetryPolicy = mkConditionalRetryPolicy None

/// CloudAtom implementation on top of Amazon DynamoDB
[<AutoSerializable(true) ; Sealed; DataContract>]
type DynamoDBAtom<'T> internal (tableName : string, account : AWSAccount, hashKey : string) =

    [<DataMember(Name = "AWSAccount")>]
    let account = account

    [<DataMember(Name = "TableName")>]
    let tableName = tableName

    [<DataMember(Name = "HashKey")>]
    let hashKey = hashKey

    [<IgnoreDataMember>]
    let mutable tableContext = None
    let getContext() =
        match tableContext with
        | Some tc -> tc
        | None ->
            let ct = TableContext.Create<AtomEntry>(account.DynamoDBClient, tableName, verifyTable = false)
            tableContext <- Some ct
            ct

    let getValueAsync () = async {
        let! item = getContext().GetItemAsync(TableKey.Hash hashKey)
        return ProcessConfiguration.BinarySerializer.Deserialize<'T>(item.Data)
    }

    interface CloudAtom<'T> with
        member __.Container = tableName
        member __.Id = hashKey

        member __.GetValueAsync () = getValueAsync()

        member __.Value = getValueAsync() |> Async.RunSync

        member __.Dispose (): Async<unit> = async {
            let! _ = getContext().DeleteItemAsync(TableKey.Hash hashKey)
            return ()
        }

        member __.ForceAsync (newValue : 'T) = async {
            let m = serialize newValue
            let uExpr = updateEntry m DateTimeOffset.Now (guid())
            let! _ = getContext().UpdateItemAsync(TableKey.Hash hashKey, uExpr)
            return ()
        }

        member __.TransactAsync (transaction : 'T -> 'R * 'T, maxRetries) = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let policy = 
                match maxRetries with
                | None -> infiniteConditionalRetryPolicy
                | Some _ -> mkConditionalRetryPolicy maxRetries

            let key = TableKey.Hash hashKey

            return! retryAsync policy <| 
                async {
                    let! oldEntry = getContext().GetItemAsync(key)
                    let oldValue = serializer.Deserialize<'T>(oldEntry.Data)
                    let returnValue, newValue = transaction oldValue
                    let m = serialize newValue

                    let newEntry = 
                        { oldEntry with 
                            Data = m
                            Timestamp = DateTimeOffset.Now
                            ETag = guid()
                        }

                    let! _ = getContext().PutItemAsync(newEntry, withEtag oldEntry.ETag)
                    return returnValue
                }
        }

/// CloudAtom provider implementation on top of Amazon DynamoDB.
[<Sealed; DataContract>]
type DynamoDBAtomProvider private (account : AWSAccount, defaultTable : string, tablePrefix : string, provisionedThroughput : int64) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "DefaultTable")>]
    let defaultTable = defaultTable

    [<DataMember(Name = "TablePrefix")>]
    let tablePrefix = tablePrefix

    [<DataMember(Name = "ProvisionedThroughput")>]
    let provisionedThroughput = provisionedThroughput

    /// <summary>
    /// Creates an AWS DynamoDB-based atom provider that
    /// connects to provided DynamoDB table.
    /// </summary>
    /// <param name="account">AWS account to be used by the provider.</param>
    /// <param name="defaultTable">Default table container.</param>
    /// <param name="provisionedThroughput">DynamoDB provision throughput. Defaults to 20.</param>
    static member internal Create (account : AWSAccount, ?defaultTable : string, ?tablePrefix : string, ?provisionedThroughput : int64) =
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

    /// <summary>
    /// Creates an AWS DynamoDB-based atom provider that
    /// connects to provided DynamoDB table.
    /// </summary>
    /// <param name="region">AWS region to be used by the provider.</param>
    /// <param name="credentials">AWS credentials to be used by the provider.</param>
    /// <param name="defaultTable">Default table container.</param>
    /// <param name="provisionedThroughput">DynamoDB provision throughput. Defaults to 20.</param>
    static member Create (region : AWSRegion, credentials : AWSCredentials, ?defaultTable : string, ?tablePrefix : string, ?provisionedThroughput : int64) =
        let account = AWSAccount.Create(region, credentials)
        DynamoDBAtomProvider.Create(account, ?defaultTable = defaultTable, ?tablePrefix = tablePrefix, ?provisionedThroughput = provisionedThroughput)

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
            Validate.tableName tableName
            let! table = TableContext.CreateAsync<AtomEntry>(account.DynamoDBClient, tableName, createIfNotExists = true)

            let m = serialize initValue
            let entry = { HashKey = atomId ; ETag = guid() ; Data = m ; Timestamp = DateTimeOffset.Now }
            let! _ = table.PutItemAsync(entry)

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