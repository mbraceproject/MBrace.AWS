namespace MBrace.AWS.Runtime.Utilities

#nowarn "1215"

open System
open System.Collections.Generic

open Nessos.FsPickler

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model

open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry
open MBrace.AWS.Runtime

[<AutoOpen>]
module internal TableConfig =
    
    [<Literal>]
    let HashKey = "HashKey"

    [<Literal>]
    let RangeKey = "RangeKey"

    [<Literal>]
    let ETag = "ETag"

[<AllowNullLiteral>]
type IDynamoDBTableEntity =
    abstract member HashKey  : string
    abstract member RangeKey : string

[<AllowNullLiteral>]
type IDynamoDBDocument =
    abstract member ToDynamoDBDocument : unit -> Document

[<AllowNullLiteral; AbstractClass; AutoSerializable(false)>]
type DynamoDBTableEntity (hashKey : string, rangeKey : string) =
    member __.HashKey  = hashKey
    member __.RangeKey = rangeKey

    interface IDynamoDBTableEntity with
        member __.HashKey  = hashKey
        member __.RangeKey = rangeKey


[<AutoOpen>]
module DynamoDBEntryExtensions =

    type DateTimeOffset with
        member dto.ToISO8601String() =
            dto.ToUniversalTime().ToString(Amazon.Util.AWSSDKUtils.ISO8601DateFormat)

    type DynamoDBEntry with
        static member op_Implicit (dtOffset : DateTimeOffset) : DynamoDBEntry =
            new Primitive(dtOffset.ToISO8601String()) :> _

        member this.AsDateTimeOffset() =
            let iso = this.AsPrimitive().AsString()
            DateTimeOffset.Parse(iso)


    let mkTableConflictRetryPolicy maxRetries interval =
        let interval = defaultArg interval 3000 |> float |> TimeSpan.FromMilliseconds
        Policy(fun retries exn ->
            if maxRetries |> Option.exists (fun mr -> retries < mr) then None
            elif StoreException.Conflict exn then Some interval
            else None)

    type IAmazonDynamoDB with
        member ddb.CreateTableIfNotExistsSafe(tableName : string, ?retryInterval : int, ?maxRetries : int) =
            retryAsync (mkTableConflictRetryPolicy maxRetries retryInterval) <| async {

                let! ct = Async.CancellationToken
                let! listedTables = ddb.ListTablesAsync(ct) |> Async.AwaitTaskCorrect
                if listedTables.TableNames |> Seq.exists(fun tn -> tn = tableName) |> not then
                    let ctr = new CreateTableRequest(TableName = tableName)
                    ctr.KeySchema.Add <| KeySchemaElement(HashKey, KeyType.HASH)
                    ctr.KeySchema.Add <| KeySchemaElement(RangeKey, KeyType.RANGE)
                    ctr.AttributeDefinitions.Add <| AttributeDefinition(HashKey, ScalarAttributeType.S)
                    ctr.AttributeDefinitions.Add <| AttributeDefinition(RangeKey, ScalarAttributeType.S)
                    ctr.ProvisionedThroughput <- new ProvisionedThroughput(10L, 10L)

                    let! _resp = ddb.CreateTableAsync(ctr, ct) |> Async.AwaitTaskCorrect
                    ()

                let rec awaitReady retries = async {
                    if retries = 0 then return failwithf "Failed to create table '%s'" tableName
                    let! descr = ddb.DescribeTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
                    if descr.Table.TableStatus <> TableStatus.ACTIVE then
                        do! Async.Sleep 1000
                        return! awaitReady (retries - 1)
                }

                do! awaitReady 20
            }


[<AutoOpen>]
module private DynamoDBUtils =
    let attributeValue (x : string) = new AttributeValue(x)

[<RequireQualifiedAccess>]
module internal Table =
    type TableConfig =
        {
            ReadThroughput  : int64
            WriteThroughput : int64
            HashKey         : string
            RangeKey        : string
        }

        static member Default = 
            {
                ReadThroughput  = 10L
                WriteThroughput = 10L
                HashKey         = HashKey
                RangeKey        = RangeKey
            }

    let writeETag (document : Document) (etag : string option) = 
        document.[ETag] <- DynamoDBEntry.op_Implicit(match etag with None -> guid() | Some e -> e)

    let readETag (document : Document) = document.[ETag].AsString()

    /// Creates a new table and wait till its status is confirmed as Active
    let createIfNotExists 
            (account : AWSAccount) 
            tableName
            (tableConfig : TableConfig option)
            (maxRetries  : int option) = async {
        let tableConfig = defaultArg tableConfig TableConfig.Default
        let maxRetries  = defaultArg maxRetries 3

        let req = CreateTableRequest(TableName = tableName)
        req.KeySchema.Add(new KeySchemaElement(tableConfig.HashKey, KeyType.HASH))
        req.KeySchema.Add(new KeySchemaElement(tableConfig.RangeKey, KeyType.RANGE))
        req.ProvisionedThroughput <- new ProvisionedThroughput(tableConfig.ReadThroughput, tableConfig.WriteThroughput)

        let create = async {
            let! ct  = Async.CancellationToken
            let! res = account.DynamoDBClient.CreateTableAsync(req, ct)
                       |> Async.AwaitTaskCorrect
                       |> Async.Catch

            match res with
            | Choice1Of2 res -> 
                return res.TableDescription.TableStatus
            | Choice2Of2 (:? ResourceInUseException) -> 
                return TableStatus.ACTIVE
            | Choice2Of2 exn -> 
                return! Async.Raise exn
        }

        let rec confirmIsActive () = async {
            let req  = DescribeTableRequest(TableName = tableName)
            let! ct  = Async.CancellationToken
            let! res = account.DynamoDBClient.DescribeTableAsync(req, ct)
                       |> Async.AwaitTaskCorrect
            if res.Table.TableStatus = TableStatus.ACTIVE
            then return ()
            else return! confirmIsActive()
        }

        let rec loop attemptsLeft = async {
            if attemptsLeft <= 0 
            then return () 
            else
                let! res = Async.Catch create
                match res with
                | Choice1Of2 status when status = TableStatus.ACTIVE -> return ()
                | Choice1Of2 _ -> do! confirmIsActive()
                | _ -> return! loop (attemptsLeft - 1)
        }

        do! loop maxRetries
    }

    let private putInternal 
            (account  : AWSAccount) 
            (tableName : string)
            (entity   : IDynamoDBDocument)
            (opConfig : UpdateItemOperationConfig option) = async { 
        let table = Table.LoadTable(account.DynamoDBClient, tableName)
        let doc   = entity.ToDynamoDBDocument()
        let! ct   = Async.CancellationToken
            
        let update = 
            match opConfig with
            | Some config -> table.UpdateItemAsync(doc, config, ct)
            | _           -> table.UpdateItemAsync(doc, ct)

        do! update
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    let put account tableName entity =
        putInternal account tableName entity None

    let putBatch 
            (account : AWSAccount) 
            (tableName : string)
            (entities : seq<#IDynamoDBDocument>) = async {
        let table = Table.LoadTable(account.DynamoDBClient, tableName)
        let batch = table.CreateBatchWrite()
        let docs  = entities |> Seq.map (fun x -> x.ToDynamoDBDocument()) 
        docs |> Seq.iter batch.AddDocumentToPut
        let! ct   = Async.CancellationToken

        do! batch.ExecuteAsync(ct)
            |> Async.AwaitTaskCorrect
    }

    let inline update< ^a when ^a : (member ETag : string option) and ^a :> IDynamoDBDocument >
        (account : AWSAccount)
        (tableName : string)
        (entity : ^a) = async {

        let! ct = Async.CancellationToken
        let table = Table.LoadTable(account.DynamoDBClient, tableName)
        let ddb = entity.ToDynamoDBDocument()
        let config = new UpdateItemOperationConfig()
        config.ReturnValues <- ReturnValues.None

        match (^a : (member ETag : string option) entity) with
        | Some etag -> 
            config.ConditionalExpression.ExpressionStatement <- sprintf "%s = :etag" ETag
            config.ConditionalExpression.ExpressionAttributeValues.[":etag"] <- DynamoDBEntry.op_Implicit etag
        | None -> ()

        let! _result = table.UpdateItemAsync(ddb, config, ct) |> Async.AwaitTaskCorrect
        ()
    }

    let delete 
            (account : AWSAccount) 
            (tableName : string)
            (entity : IDynamoDBDocument) = async {
        let table = Table.LoadTable(account.DynamoDBClient, tableName)
        let! ct   = Async.CancellationToken
        do! table.DeleteItemAsync(entity.ToDynamoDBDocument(), ct)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    let deleteBatch
            (account : AWSAccount) 
            (tableName : string)
            (entities : seq<#IDynamoDBDocument>) = async {
        let table = Table.LoadTable(account.DynamoDBClient, tableName)
        let batch = table.CreateBatchWrite()
        let docs  = entities |> Seq.map (fun x -> x.ToDynamoDBDocument()) 
        docs |> Seq.iter batch.AddItemToDelete
        let! ct   = Async.CancellationToken

        do! batch.ExecuteAsync(ct)
            |> Async.AwaitTaskCorrect
    }

    let inline queryDocs (account : AWSAccount) (mkRequest : unit -> QueryRequest) (mapper : Document -> 'a)  = async {
        let results = ResizeArray<_>()
        let rec loop lastKey = async {
            let qr = mkRequest()
            qr.ExclusiveStartKey <- lastKey

            let! ct  = Async.CancellationToken
            let! res = account.DynamoDBClient.QueryAsync(qr, ct)
                        |> Async.AwaitTaskCorrect

            res.Items 
            |> Seq.map (Document.FromAttributeMap >> mapper)
            |> results.AddRange

            if res.LastEvaluatedKey.Count > 0 then
                do! loop res.LastEvaluatedKey
        }

        do! loop (Dictionary<string, AttributeValue>())

        return results :> ICollection<_>
    }

    let inline query< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a) > 
            (account : AWSAccount) 
            tableName 
            (hashKey : string) = async {

        let mkRequest() =
            let req = QueryRequest(TableName = tableName)
            let eqCond = new Condition()
            eqCond.ComparisonOperator <- ComparisonOperator.EQ
            eqCond.AttributeValueList.Add(new AttributeValue(hashKey))
            req.KeyConditions.Add(HashKey, eqCond)
            req

        return! queryDocs account mkRequest (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
    }

    let private readInternal 
            (account : AWSAccount) 
            (tableName : string)
            (hashKey : string) 
            (rangeKey : string) = async {
        let req = GetItemRequest(TableName = tableName)
        req.Key.Add(HashKey,  new AttributeValue(hashKey))
        req.Key.Add(RangeKey, new AttributeValue(rangeKey))

        let! ct  = Async.CancellationToken
        let! res = account.DynamoDBClient.GetItemAsync(req, ct)
                   |> Async.AwaitTaskCorrect
        return res
    }

    let inline read< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a) > 
            (account : AWSAccount) 
            (tableName : string)
            (hashKey : string) 
            (rangeKey : string) = async {
        let! res = readInternal account tableName hashKey rangeKey

        return Document.FromAttributeMap res.Item 
               |> (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
    }

    let inline increment 
            (account : AWSAccount) 
            tableName 
            hashKey
            rangeKey
            fieldToIncr = async {
        // see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions
        let req  = UpdateItemRequest(TableName = tableName)
        req.Key.Add(HashKey,  attributeValue hashKey)
        req.Key.Add(RangeKey, attributeValue rangeKey)
        req.ExpressionAttributeNames.Add("#F", fieldToIncr)
        req.ExpressionAttributeValues.Add(":val", attributeValue "1")
        req.UpdateExpression <- "SET #F = #F+:val"
        req.ReturnValues <- ReturnValue.UPDATED_NEW

        let! ct  = Async.CancellationToken
        let! res = account.DynamoDBClient.UpdateItemAsync(req, ct)
                    |> Async.AwaitTaskCorrect
        
        let newValue = res.Attributes.[fieldToIncr]
        return System.Int64.Parse newValue.N
    }

    let inline transact< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a)
                            and  ^a :> IDynamoDBDocument >
            (account  : AWSAccount) 
            (tableName : string)
            (hashKey  : string) 
            (rangeKey : string)
            (conditionalField : ^a -> string * string option) // used to perform conditional update
            (update   : ^a -> ^a) = async {
        let read () = async {
            let! res = readInternal account tableName hashKey rangeKey
            return Document.FromAttributeMap res.Item 
                   |> (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
        }

        let rec transact x = async {
            let x' = update x
            
            let config = UpdateItemOperationConfig()
            let (condField, condFieldVal) = conditionalField x
            match condFieldVal with
            | Some x ->
                config.Expected.Add(condField, DynamoDBEntry.op_Implicit x)
            | _ -> 
                config.ConditionalExpression.ExpressionAttributeNames.Add("#F", condField)
                config.ConditionalExpression.ExpressionStatement <- "attribute_not_exists(#F)"

            let! res = Async.Catch <| putInternal account tableName x' (Some config)
            match res with
            | Choice1Of2 _ -> return x'
            | Choice2Of2 (:? ConditionalCheckFailedException) ->
                let! x = read()
                return! transact x
            | Choice2Of2 exn -> return raise exn
        }
        
        let! x = read()
        return! transact x
    }

    let inline readStringOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then doc.[fieldName].AsString() else null

    let inline readIntOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsInt() else nullableDefault<int>
    
    let inline readInt64OrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsLong() else nullableDefault<int64>
    
    let inline readBoolOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsBoolean() else nullableDefault<bool>

    let inline readDateTimeOffsetOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName 
        then nullable <| doc.[fieldName].AsDateTimeOffset() 
        else nullableDefault<DateTimeOffset>

    let inline readByteArrayOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then doc.[fieldName].AsByteArray() else null

    let inline readDoubleOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName 
        then nullable <| doc.[fieldName].AsDouble() 
        else nullableDefault<double>