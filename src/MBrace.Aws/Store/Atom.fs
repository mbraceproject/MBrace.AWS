namespace MBrace.Aws.Store

open System.IO
open System.Runtime.Serialization

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Aws.Runtime

/// CloudAtom implementation on top of Amazon DynamoDB
[<AutoSerializable(true) ; Sealed; DataContract>]
type DynamoDBAtom<'T> internal 
        (tableName : string, 
         account   : AwsDynamoDBAccount,
         hashKey   : string) =
    [<DataMember(Name = "DynamoDBAccount")>]
    let account = account

    [<DataMember(Name = "TableName")>]
    let tableName = tableName

    [<DataMember(Name = "HashKey")>]
    let hashKey = hashKey

    let getValueAsync() = async {
        let req = GetItemRequest(TableName = tableName)
        req.Key.Add("HashKey", new AttributeValue(hashKey))
        req.AttributesToGet.Add("Blob")

        let! res = account.DynamoDBClient.GetItemAsync(req)
                   |> Async.AwaitTaskCorrect

        let blob = res.Item.["Blob"].B.ToArray()
        return ProcessConfiguration.BinarySerializer.UnPickle<'T>(blob)
    }

    interface CloudAtom<'T> with
        member __.Container = tableName
        member __.Id = hashKey

        member __.GetValueAsync () = getValueAsync()

        member __.Value = getValueAsync() |> Async.RunSync

        member __.Dispose (): Async<unit> = async {
            let req = DeleteItemRequest(TableName = tableName)
            req.Key.Add("HashKey", new AttributeValue(hashKey))
            do! account.DynamoDBClient.DeleteItemAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.ForceAsync (newValue : 'T) = async {
            let newBinary = ProcessConfiguration.BinarySerializer.Pickle newValue
            
            let req = UpdateItemRequest(TableName = tableName)
            req.Key.Add("HashKey", new AttributeValue(hashKey))

            let newBlobAttr = new AttributeValue()
            newBlobAttr.B <- new MemoryStream(newBinary)
            let attrUpdate = new AttributeValueUpdate(newBlobAttr, AttributeAction.PUT)
            req.AttributeUpdates.Add("Blob", attrUpdate)

            do! account.DynamoDBClient.UpdateItemAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.TransactAsync (transaction, maxRetries) = async {
            failwith "not implemented yet"
        }