namespace MBrace.Aws.Runtime.Utilities

open System
open System.Collections.Generic

open Nessos.FsPickler

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<AllowNullLiteral>]
type IDynamoDBTableEntity =
    abstract member HashKey  : string
    abstract member RangeKey : string

[<AllowNullLiteral; AbstractClass>]
type DynamoDBTableEntity (hashKey, rangeKey) =
    member val HashKey  : string = hashKey with get
    member val RangeKey : string = rangeKey with get

    interface IDynamoDBTableEntity with
        member __.HashKey  = hashKey
        member __.RangeKey = rangeKey

[<AllowNullLiteral>]
type IDynamoDBDocument =
    abstract member ToDynamoDBDocument : unit -> Document

[<AutoOpen>]
module DynamoDBEntryExtensions =
    type DynamoDBEntry with
        static member op_Implicit (dtOffset : DateTimeOffset) : DynamoDBEntry =           
            let entry = new Primitive(dtOffset.ToString())
            entry :> DynamoDBEntry

        member this.AsDateTimeOffset() =
            DateTimeOffset.Parse <| this.AsPrimitive().AsString()

[<RequireQualifiedAccess>]
module internal Table =
    // NOTE: implement a specific put rather than reinvent the object persistence layer as that's a lot
    // of work and at this point not enough payoff
    let put (account : AwsDynamoDBAccount) tableName (entity : IDynamoDBDocument) =
        async { 
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let doc   = entity.ToDynamoDBDocument()
            let! ct   = Async.CancellationToken

            do! table.UpdateItemAsync(doc, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

    let putBatch 
            (account : AwsDynamoDBAccount) 
            tableName 
            (entities : 'a seq when 'a :> IDynamoDBDocument) =
        async {
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let batch = table.CreateBatchWrite()
            let docs  = entities |> Seq.map (fun x -> x.ToDynamoDBDocument()) 
            docs |> Seq.iter batch.AddDocumentToPut
            let! ct   = Async.CancellationToken

            do! batch.ExecuteAsync(ct)
                |> Async.AwaitTaskCorrect
        }

    let delete 
            (account : AwsDynamoDBAccount) 
            tableName 
            (entity : IDynamoDBDocument) =
        async {
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let! ct   = Async.CancellationToken
            do! table.DeleteItemAsync(entity.ToDynamoDBDocument(), ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

    let deleteBatch
            (account : AwsDynamoDBAccount) 
            tableName 
            (entities : 'a seq when 'a :> IDynamoDBDocument) =
        async {
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let batch = table.CreateBatchWrite()
            let docs  = entities |> Seq.map (fun x -> x.ToDynamoDBDocument()) 
            docs |> Seq.iter batch.AddItemToDelete
            let! ct   = Async.CancellationToken

            do! batch.ExecuteAsync(ct)
                |> Async.AwaitTaskCorrect
        }

    let inline query< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a) > 
            (account : AwsDynamoDBAccount) 
            tableName 
            (hashKey : string) = async {
        let results = ResizeArray<_>()

        let rec loop lastKey =
            async {
                let req = QueryRequest(TableName = tableName)
                let eqCond = new Condition()
                eqCond.ComparisonOperator <- ComparisonOperator.EQ
                eqCond.AttributeValueList.Add(new AttributeValue(hashKey))
                req.KeyConditions.Add("HashKey", eqCond)
                req.ExclusiveStartKey <- lastKey

                let! ct  = Async.CancellationToken
                let! res = account.DynamoDBClient.QueryAsync(req, ct)
                           |> Async.AwaitTaskCorrect

                res.Items 
                |> Seq.map Document.FromAttributeMap 
                |> Seq.map (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
                |> results.AddRange

                if res.LastEvaluatedKey.Count > 0 then
                    do! loop res.LastEvaluatedKey
            }
        do! loop (Dictionary<string, AttributeValue>())

        return results :> ICollection<_>
    }

    let private readInternal 
            (account : AwsDynamoDBAccount) 
            tableName 
            (hashKey : string) 
            (rangeKey : string) = async {
        let req = GetItemRequest(TableName = tableName)
        req.Key.Add("HashKey",  new AttributeValue(hashKey))
        req.Key.Add("RangeKey", new AttributeValue(rangeKey))

        let! ct  = Async.CancellationToken
        let! res = account.DynamoDBClient.GetItemAsync(req, ct)
                   |> Async.AwaitTaskCorrect
        return res
    }

    let inline read< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a) > 
            (account : AwsDynamoDBAccount) 
            tableName 
            (hashKey : string) 
            (rangeKey : string) = async {
        let! res = readInternal account tableName hashKey rangeKey

        return Document.FromAttributeMap res.Item 
               |> (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
    }

    let inline transact2< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a)
                             and  ^a :> IDynamoDBDocument >
            (account : AwsDynamoDBAccount) 
            tableName 
            (hashKey : string) 
            (rangeKey : string)
            (update : ^a -> ^a) = async {
        let read () = async {
            let! res = readInternal account tableName hashKey rangeKey
            return Document.FromAttributeMap res.Item 
                   |> (fun d -> (^a : (static member FromDynamoDBDocument : Document -> ^a) d))
        }

        let rec transact x = async {
            let x' = update x
            let! res = Async.Catch <| put account tableName x'
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