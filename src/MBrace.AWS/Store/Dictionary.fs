namespace MBrace.AWS.Store

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization
open System.Text.RegularExpressions

open Amazon.Runtime
open FSharp.AWS.DynamoDB

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils.Retry
open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

[<AutoOpen>]
module private DynamoDBDictionaryUtils =

    type DictEntry =
        {
            [<HashKey; CustomName("HashKey")>]
            DictionaryId : string

            [<RangeKey; CustomName("RangeKey")>]
            EntryId : string

            Data : MemoryStream

            ETag : string

            Timestamp : DateTimeOffset
        }

    let dictEntry = RecordTemplate.Define<DictEntry> ()

    let withEtag = dictEntry.PrecomputeConditionalExpr <@ fun etag ae -> ae.ETag = etag @>

    let updateEntry = 
        dictEntry.PrecomputeUpdateExpr                             
                    <@ fun data time etag (r:DictEntry) -> 
                            { r with 
                                Data = data
                                Timestamp = time
                                ETag = etag } @>

    let queryEntries =
        dictEntry.PrecomputeConditionalExpr <@ fun dictId (r:DictEntry) -> r.DictionaryId = dictId @>


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

/// MBrace CloudDictionary implementation on top of Azure Table Store.
[<AutoSerializable(true) ; Sealed; DataContract>]
type DynamoDBDictionary<'T> internal (tableName : string, dictId : string, account : AWSAccount) = 
    
    [<DataMember(Name = "Account")>]
    let account = account
    [<DataMember(Name = "Table")>]
    let tableName = tableName
    [<DataMember(Name = "DictionaryId")>]
    let dictId = dictId

    [<IgnoreDataMember>]
    let mutable tableContext = None
    let getContext() =
        match tableContext with
        | Some tc -> tc
        | None ->
            let ct = TableContext.Create<DictEntry>(account.DynamoDBClient, tableName, verifyTable = false)
            tableContext <- Some ct
            ct

    let getEntitiesAsync() = 
        getContext().QueryAsync(queryEntries dictId)

    let getKey (entryId : string) = TableKey.Combined(dictId, entryId)

    let getSeqAsync() = async {
        let serializer = ProcessConfiguration.BinarySerializer
        let! entities = getEntitiesAsync()
        return entities |> Seq.map (fun e -> new KeyValuePair<_,_>(e.EntryId, serializer.Deserialize<'T>(e.Data)))
    }

    let getSeq() = getSeqAsync() |> Async.RunSync

    interface seq<KeyValuePair<string,'T>> with
        member __.GetEnumerator() = getSeq().GetEnumerator() :> IEnumerator
        member __.GetEnumerator() = getSeq().GetEnumerator()
        
    interface CloudDictionary<'T> with
        member x.IsKnownCount: bool = false
        member x.IsMaterialized : bool = false
        member x.IsKnownSize: bool = false
        
        member this.ForceAddAsync(key: string, value : 'T): Async<unit> = async {
            let ms = serialize value
            let uexpr = updateEntry ms DateTimeOffset.Now (guid())
            let! _ = getContext().UpdateItemAsync(getKey key, uexpr)
            return ()
        }
        
        member this.TransactAsync(key: string, transacter: 'T option -> 'R * 'T, maxRetries: int option): Async<'R> = async {
            let serializer = ProcessConfiguration.BinarySerializer
            let table = getContext()
            let rec aux retries = async {
                if retries = 0 then failwith "Maximum number of retries exceeded."

                let! result = table.GetItemAsync(getKey key) |> Async.Catch
                match result with
                | Choice1Of2 oldEntry ->
                    let oldValue = serializer.Deserialize(oldEntry.Data)
                    let result, newValue = transacter (Some oldValue)
                    let ms = serialize newValue
                    let newEntry = 
                        { oldEntry with
                            Data = ms
                            Timestamp = DateTimeOffset.Now
                            ETag = guid() }

                    let! res = table.PutItemAsync(newEntry, precondition = withEtag oldEntry.ETag) |> Async.Catch
                    match res with
                    | Choice1Of2 _ -> return result
                    | Choice2Of2 (:? ConditionalCheckFailedException) -> return! aux (retries - 1)
                    | Choice2Of2 e -> return! Async.Raise e

                | Choice2Of2(:? Amazon.DynamoDBv2.Model.ResourceNotFoundException) ->
                    let result, newValue = transacter None
                    let ms = serialize newValue
                    let newEntry = {
                        DictionaryId = dictId
                        EntryId = key
                        Data = ms
                        Timestamp = DateTimeOffset.Now
                        ETag = guid() }

                    let! res = table.PutItemAsync(newEntry, precondition = dictEntry.ItemDoesNotExist) |> Async.Catch
                    match res with
                    | Choice1Of2 _ -> return result
                    | Choice2Of2 (:? ConditionalCheckFailedException) -> return! aux (retries - 1)
                    | Choice2Of2 e -> return! Async.Raise e

                | Choice2Of2 e -> return! Async.Raise e
            }

            return! aux (defaultArg maxRetries -1)
        }

        member this.ContainsKeyAsync(key: string): Async<bool> = async {
            return! getContext().ContainsKeyAsync(getKey key)
        }
        
        member this.GetCountAsync () : Async<int64> = async { let! entities = getEntitiesAsync() in return int64 entities.Length }
        member this.GetSizeAsync () : Async<int64> = async { let! entities = getEntitiesAsync() in return int64 entities.Length }

        member this.Dispose(): Async<unit> = async {
            let table = getContext()
            let! entities = getEntitiesAsync()
            do!
                entities 
                |> Seq.map table.Template.ExtractKey
                |> Seq.chunksOf 25
                |> Seq.map table.BatchDeleteItemsAsync
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member this.Id: string = sprintf "table:%s, id:%s" tableName dictId
        
        member this.RemoveAsync(key: string): Async<bool> = async {
            try 
                let! _ = getContext().DeleteItemAsync(getKey key)
                return true
            with :? Amazon.DynamoDBv2.Model.ResourceNotFoundException -> return false
        }
        
        member this.GetEnumerableAsync(): Async<seq<Collections.Generic.KeyValuePair<string,'T>>> = async {
            return! getSeqAsync()
        }
        
        member this.TryAddAsync(key: string, value: 'T): Async<bool> = async {
            let ms = serialize value
            let entry = {
                DictionaryId = dictId
                EntryId = key
                Data = ms
                Timestamp = DateTimeOffset.Now
                ETag = guid() }

            try
                let! _ = getContext().PutItemAsync(entry, precondition = template.ItemDoesNotExist)
                return true
            with :? ConditionalCheckFailedException -> return false
        }
        
        member x.TryFindAsync(key: string): Async<'T option> = async {
            try
                let! entry = getContext().GetItemAsync(getKey key)
                let value = ProcessConfiguration.BinarySerializer.Deserialize<'T> entry.Data
                return Some value
            with _ -> return None
        }

/// CloudDictionary provider implementation on top of Azure table store.
[<Sealed; DataContract>]
type DynamoDBDictionaryProvider private (account : AWSAccount, tableName : string) =
    
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "TableName")>]
    let tableName = tableName

    [<IgnoreDataMember>]
    let mutable tableContext = None
    let getContext() =
        match tableContext with
        | Some tc -> tc
        | None ->
            let ct = TableContext.Create<DictEntry>(account.DynamoDBClient, tableName, verifyTable = false)
            tableContext <- Some ct
            ct

    do getContext().VerifyTable(createIfNotExists = true)

    member __.DeleteTableAsync() = async {
        let! ct = Async.CancellationToken
        let! _ = getContext().Client.DeleteTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
        return ()
    }

    /// <summary>
    ///     Creates a TableDirectionaryProvider instance using provided Azure storage account.
    /// </summary>
    /// <param name="account">Azure storage account.</param>
    static member internal Create(account : AWSAccount, ?tableName : string) =
        ignore account.Credentials // ensure that connection string is present in the current context
        let tableName = match tableName with None -> getRandomTableName "cloudDict" | Some tn -> Validate.tableName tn; tn
        new DynamoDBDictionaryProvider(account, tableName)

    /// <summary>
    ///     Creates a TableDirectionaryProvider instance using provided Azure storage account.
    /// </summary>
    /// <param name="region">AWS region to be used by the provider.</param>
    /// <param name="credentials">AWS credentials to be used by the provider.</param>
    static member Create(region : AWSRegion, credentials : AWSCredentials, ?tableName : string) =
        let account = AWSAccount.Create(region, credentials)
        DynamoDBDictionaryProvider.Create(account, ?tableName = tableName)

    interface ICloudDictionaryProvider with
        member x.Id = tableName
        member x.Name = "AWS DynamoDB CloudDictionary Provider"
        member x.GetRandomDictionaryId() = "cloudDictionary-" + guid()
        member x.CreateDictionary<'T>(dictId : string): Async<CloudDictionary<'T>> = async {
            return new DynamoDBDictionary<'T>(tableName, dictId, account) :> CloudDictionary<'T>
        }

        member x.GetDictionaryById<'T>(dictId : string) : Async<CloudDictionary<'T>> = async {
            return new DynamoDBDictionary<'T>(tableName, dictId, account) :> CloudDictionary<'T>
        }

        member x.IsSupportedValue(value: 'T): bool = 
            let size = ProcessConfiguration.BinarySerializer.ComputeSize value
            size <= maxPayload