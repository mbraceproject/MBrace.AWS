namespace MBrace.Aws.Runtime.Utilities

open System
open System.Collections.Generic

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<AllowNullLiteral>]
type IDynamoDBDocument =
    abstract member ToDynamoDBDocument : unit -> Document

[<RequireQualifiedAccess>]
module internal Table =
    // NOTE: implement a specific put rather than reinvent the object persistence layer as that's a lot
    // of work and at this point not enough payoff
    let update (account : AwsDynamoDBAccount) tableName (entity : IDynamoDBDocument) =
        async { 
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let doc   = entity.ToDynamoDBDocument()
            let! ct   = Async.CancellationToken

            do! table.UpdateItemAsync(doc, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

    let inline query< ^a when ^a : (static member FromDynamoDBDocument : Document -> ^a) > 
            (account : AwsDynamoDBAccount) tableName (id : string) = async {
        let results = ResizeArray<_>()

        let rec loop lastKey =
            async {
                let req = QueryRequest(TableName = tableName)
                let eqCond = new Condition()
                eqCond.ComparisonOperator <- ComparisonOperator.EQ
                eqCond.AttributeValueList.Add(new AttributeValue(id))
                req.KeyConditions.Add("Id", eqCond)
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

    let inline ReadStringOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then doc.[fieldName].AsString() else null

    let inline ReadIntOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsInt() else nullableDefault<int>
    
    let inline ReadInt64OrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsLong() else nullableDefault<int64>
    
    let inline ReadBoolOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsBoolean() else nullableDefault<bool>

    let inline ReadDateTimeOrDefault (doc : Document) fieldName =
        if doc.ContainsKey fieldName then nullable <| doc.[fieldName].AsDateTime() else nullableDefault<DateTime>