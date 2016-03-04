namespace MBrace.AWS.Runtime

open System
open System.Runtime.Serialization

open MBrace.Runtime
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open FSharp.DynamoDB

[<AutoOpen>]
module private CounterImpl =

    [<ConstantRangeKey("RangeKey", "Counter")>]
    type CounterEntity = 
        { 
            [<HashKey; CustomName("HashKey")>]
            Id : string

            Value : int64
        }

    let private template = RecordTemplate.Define<CounterEntity>()

    let incrExpr = template.PrecomputeUpdateExpr <@ fun e -> { e with Value = e.Value + 1L } @>

/// Defines a distributed counter for use by the MBrace execution engine
/// using DynamoDB
[<DataContract; Sealed>]
type internal TableCounter (clusterId : ClusterId, hashKey : string) =

    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "HashKey")>] hashKey = hashKey

    let getTable() = clusterId.GetRuntimeTable<CounterEntity>()

    interface ICloudCounter with
        member x.Dispose() = async {
            let! _ = getTable().DeleteItemAsync(TableKey.Hash hashKey)
            return ()
        }
        
        member x.Increment() = async { 
            let! entity = getTable().UpdateItemAsync(TableKey.Hash hashKey, incrExpr)
            return entity.Value
        }

        member x.Value = async {
            let! entity = getTable().GetItemAsync(TableKey.Hash hashKey)
            return entity.Value
        }


[<Sealed>]
type DynamoDBCounterFactory private (clusterId : ClusterId) =

    let getTable() = clusterId.GetRuntimeTable<CounterEntity>()

    interface ICloudCounterFactory with
        member x.CreateCounter(initialValue: int64) = async {
            let id = "counter:" + guid()
            let entry = { Id = id ; Value = initialValue }
            let! _ = getTable().PutItemAsync(entry)
            return new TableCounter(clusterId, id) :> ICloudCounter
        }

    static member Create(clusterId : ClusterId) = 
        new DynamoDBCounterFactory(clusterId)