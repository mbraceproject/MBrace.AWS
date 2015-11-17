namespace MBrace.Aws.Runtime

open System
open System.Runtime.Serialization

open MBrace.Runtime
open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities
open MBrace.Aws

open Amazon.DynamoDBv2.DocumentModel

// Defines a distributed counter for use by the MBrace execution engine
// using DynamoDB

// NOTE : All types that inherit TableEntity must provide a default public ctor.
type CounterEntity(id : string, value : int64) = 
    inherit DynamoDBTableEntity(id, CounterEntity.DefaultRangeKey)
    member val Counter = value with get, set

    new () = new CounterEntity(null, 0L)

    static member DefaultRangeKey = "CounterEntity"

    static member FromDynamoDBDocument (doc : Document) = 
        let hashKey = doc.["HashKey"].AsString()
        let value   = doc.["Counter"].AsLong()

        new CounterEntity(hashKey, value)

    interface IDynamoDBDocument with
        member this.ToDynamoDBDocument () =
            let doc = new Document()

            doc.["HashKey"]  <- DynamoDBEntry.op_Implicit(this.HashKey)
            doc.["RangeKey"] <- DynamoDBEntry.op_Implicit(this.RangeKey)
            doc.["Counter"]  <- DynamoDBEntry.op_Implicit(this.Counter)

            doc

[<DataContract; Sealed>]
type internal TableCounter (clusterId : ClusterId, hashKey : string) =
    let [<DataMember(Name = "ClusterId")>] id = clusterId
    let [<DataMember(Name = "HashKey")>] hashKey = hashKey

    interface ICloudCounter with
        member x.Dispose() = async {
            let counter = CounterEntity(hashKey, 0L)
            do! Table.delete 
                    id.DynamoDBAccount 
                    id.RuntimeTable 
                    counter
        }
        
        member x.Increment() = async { 
            return! Table.increment
                        id.DynamoDBAccount 
                        id.RuntimeTable 
                        hashKey 
                        CounterEntity.DefaultRangeKey
                        "Counter"
        }

        member x.Value = async {
            let! entity = 
                Table.read<CounterEntity> 
                    id.DynamoDBAccount 
                    id.RuntimeTable 
                    hashKey 
                    CounterEntity.DefaultRangeKey
            return entity.Counter
        }

[<Sealed>]
type TableCounterFactory private (clusterId : ClusterId) =
    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int64) = async {
            let record = new CounterEntity(guid(), initialValue)
            do! Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable record
            return new TableCounter(clusterId, record.HashKey) :> ICloudCounter
        }

    static member Create(clusterId : ClusterId) = 
        new TableCounterFactory(clusterId)