namespace MBrace.Aws.Store

open System
open System.IO
open System.Runtime.Serialization

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<AutoOpen>]
module private DynamoDBAtomUtils =
    let random = new Random(int DateTime.UtcNow.Ticks)
    let randOf (x : char []) = x.[random.Next(0, x.Length)]
    let alpha = [|'a'..'z'|]
    let alphaNumeric = Array.append alpha [|'0'..'9'|]
    
    let randomTableName () =
        let name = 
            [| yield randOf alpha
               for _i = 1 to 64 do 
                   yield randOf alphaNumeric |]

        new String(name)

    // max item size is 400KB including attribute length, etc.
    // allow 1KB for all that leaves 399KB for actula payload
    let maxPayload = 399L * 1024L

    let timestamp () = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.zzzz")

    let attrValueString (x : string) = new AttributeValue(x)
    let attrValueBytes (bytes : byte[]) = 
        let attrValue = new AttributeValue()
        attrValue.B <- new MemoryStream(bytes)
        attrValue

    let expectedAttrValueString (x : string) =
        new ExpectedAttributeValue(attrValueString x)
    let expectedNotExists () =
        new ExpectedAttributeValue(false)

    let updateAttrValueString (x : string) =
        new AttributeValueUpdate(attrValueString x, AttributeAction.PUT)
    let updateAttrValueBytes (bytes : byte[]) =
        new AttributeValueUpdate(attrValueBytes bytes, AttributeAction.PUT)

/// CloudAtom implementation on top of Amazon DynamoDB
[<AutoSerializable(true) ; Sealed; DataContract>]
type DynamoDBAtom<'T> internal 
        (tableName : string, 
         account   : AwsAccount,
         hashKey   : string) =
    [<DataMember(Name = "DynamoDBAccount")>]
    let account = account

    [<DataMember(Name = "TableName")>]
    let tableName = tableName

    [<DataMember(Name = "HashKey")>]
    let hashKey = hashKey

    let getItemAsync () = async {
        let req = GetItemRequest(TableName = tableName)
        req.Key.Add("HashKey", attrValueString hashKey)

        return! account.DynamoDBClient.GetItemAsync(req)
                |> Async.AwaitTaskCorrect
    }

    let getValueAsync () = async {
        let! res = getItemAsync()
        let blob = res.Item.["Blob"].B.ToArray()
        return ProcessConfiguration.BinarySerializer.UnPickle<'T>(blob)
    }

    /// Default function for calcuating delay (in milliseconds) between retries
    /// based on (http://en.wikipedia.org/wiki/Exponential_backoff)
    /// After 8 retries the delay starts to become unreasonable for most 
    /// scenarios, so cap the delay at that
    let exponentialDelay =
        let calcDelay retries = 
            let rec sum acc = function | 0 -> acc | n -> sum (acc + n) (n - 1)

            let n = pown 2 retries - 1
            let slots = float (sum 0 n) / float (n + 1)
            int (100.0 * slots)

        let delays = [| 0..8 |] |> Array.map calcDelay

        (fun retries -> delays.[min retries 8])

    let updateReq (newBlob : byte[]) = 
        let req = UpdateItemRequest(TableName = tableName)
        req.Key.Add("HashKey", attrValueString hashKey)
        req.AttributeUpdates.Add("Blob", updateAttrValueBytes newBlob)

        req

    interface CloudAtom<'T> with
        member __.Container = tableName
        member __.Id = hashKey

        member __.GetValueAsync () = getValueAsync()

        member __.Value = getValueAsync() |> Async.RunSync

        member __.Dispose (): Async<unit> = async {
            let req = DeleteItemRequest(TableName = tableName)
            req.Key.Add("HashKey", attrValueString hashKey)
            do! account.DynamoDBClient.DeleteItemAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.ForceAsync (newValue : 'T) = async {
            let req = 
                newValue
                |> ProcessConfiguration.BinarySerializer.Pickle 
                |> updateReq

            do! account.DynamoDBClient.UpdateItemAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }

        member __.TransactAsync (transaction, maxRetries) = async {
            let serializer = ProcessConfiguration.BinarySerializer

            // TODO : is infinite retry really the right thing to do?
            let maxRetries = defaultArg maxRetries Int32.MaxValue

            let rec update count = async {
                if count >= maxRetries then
                    return raise <| exn("Maximum number of retries exceeded.")
                else
                    let! oldItem = getItemAsync()
                    let oldTimestamp = oldItem.Item.["LastModified"].S
                    let oldBlob  = oldItem.Item.["Blob"].B.ToArray()
                    let oldValue = serializer.UnPickle<'T>(oldBlob)
                    let returnValue, newValue = transaction oldValue
                    let newBinary = serializer.Pickle newValue
                    
                    let req = updateReq newBinary
                    req.Expected.Add(
                        "LastModified", 
                        expectedAttrValueString oldTimestamp)
                    req.AttributeUpdates.Add(
                        "LastModified", 
                        updateAttrValueString <| timestamp())

                    let! res = 
                        account.DynamoDBClient.UpdateItemAsync(req)
                        |> Async.AwaitTaskCorrect
                        |> Async.Catch

                    match res with
                    | Choice1Of2 _ -> return returnValue
                    | Choice2Of2 (:? ConditionalCheckFailedException) -> 
                        do! Async.Sleep <| exponentialDelay count
                        return! update (count+1)
                    | Choice2Of2 exn -> return raise exn
            }

            return! update 0
        }

/// CloudAtom provider implementation on top of Amazon DynamoDB.
[<Sealed; DataContract>]
type DynamoDBAtomProvider private 
        (account : AwsAccount, 
         defaultTable : string) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "DefaultTable")>]
    let defaultTable = defaultTable

    /// Creates an AWS DynamoDB-based atom provider that
    /// connects to provided DynamoDB table.
    /// </summary>
    static member Create 
            (account : AwsAccount, 
             ?defaultTable : string) =
        let defaultTable = 
            match defaultTable with
            | Some x -> x
            | _ -> randomTableName()
        new DynamoDBAtomProvider(account, defaultTable)
        
    interface ICloudAtomProvider with
        member __.Id = sprintf "arn:aws:dynamodb:table/%s" defaultTable
        member __.Name = "AWS DynamoDB CloudAtom Provider"
        member __.DefaultContainer = defaultTable

        member __.WithDefaultContainer (tableName : string) = 
            new DynamoDBAtomProvider(account, tableName) :> _

        member __.IsSupportedValue(value) = 
            let size = ProcessConfiguration.BinarySerializer.ComputeSize value
            size <= maxPayload

        member __.CreateAtom<'T>(tableName, atomId, initValue) = async {
            Validate.tableName tableName
            let binary = ProcessConfiguration.BinarySerializer.Pickle initValue

            let req = PutItemRequest(TableName = tableName)
            req.Item.Add("HashKey",      attrValueString atomId)
            req.Item.Add("Blob",         attrValueBytes binary)
            req.Item.Add("LastModified", attrValueString <| timestamp())

            // atomically add a new Atom only if one doesn't exist with the same ID
            req.Expected.Add("LastModified", expectedNotExists())
            
            do! account.DynamoDBClient.PutItemAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore

            return new DynamoDBAtom<'T>(tableName, account, atomId) :> CloudAtom<'T>
        }
        
        member __.GetAtomById(tableName, atomId) = async {
            return new DynamoDBAtom<'T>(tableName, account, atomId) :> CloudAtom<'T>
        }

        member __.GetRandomAtomIdentifier() = 
            sprintf "cloudAtom-%s" <| mkUUID()
        member __.GetRandomContainerName() = 
            "cloudAtom" + randomTableName()

        member __.DisposeContainer(tableName) = async {
            let req = DeleteTableRequest(TableName = tableName)
            do! account.DynamoDBClient.DeleteTableAsync(req)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }