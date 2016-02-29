namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Runtime.Serialization

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open FSharp.DynamoDB

 [<AutoOpen>]
 module private ResultAggregatorImpl =

    [<ConstantRangeKey("RangeKey", "ResultAggregator")>]
    type ResultAggregatorEntry =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            Capacity : int

            AggregatedUris : Map<string, string>
        }

    let template = RecordTemplate.Define<ResultAggregatorEntry>()

    let entryNotExists = 
        <@ fun id r -> r.AggregatedUris |> Map.containsKey id |> not @>
        |> template.PrecomputeConditionalExpr

    let addEntry =
        <@ fun id path r -> { r with AggregatedUris = r.AggregatedUris |> Map.add id path } @>
        |> template.PrecomputeUpdateExpr

[<DataContract; Sealed>]
type ResultAggregator<'T> internal (clusterId : ClusterId, hashKey : string, size : int) =
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "HashKey")>] partitionKey = hashKey
    let [<DataMember(Name = "Size")>] size = size

    let getTable() = clusterId.GetRuntimeTable<ResultAggregatorEntry>()

    member this.UUID = hashKey

    interface ICloudResultAggregator<'T> with
        member this.Capacity: int = size
        
        member this.CurrentSize: Async<int> = async {
            let! item = getTable().GetItemAsync(TableKey.Hash hashKey)
            return item.AggregatedUris.Count
        }
        
        member this.Dispose(): Async<unit> = async {
            let table = getTable()
            let! deleted = table.DeleteItemAsync(TableKey.Hash hashKey)
            do! 
                deleted.AggregatedUris
                |> Seq.map (fun kv -> S3Persist.DeletePersistedClosure(clusterId, kv.Value))
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member this.IsCompleted: Async<bool> = async {
            let! currentSize = (this :> ICloudResultAggregator<'T>).CurrentSize
            return currentSize = size
        }
        
        member this.SetResult(index: int, value: 'T, _workerId : IWorkerId): Async<bool> = async { 
            let uri = sprintf "%s/%s" partitionKey (guid())
            do! S3Persist.PersistClosure(clusterId, value, uri, allowNewSifts = false)
            let id = sprintf "item%d" index
            let! item = getTable().UpdateItemAsync(TableKey.Hash hashKey, 
                                                    addEntry id uri, 
                                                    precondition = entryNotExists id)

            return item.AggregatedUris.Count = size
        }
        
        member this.ToArray(): Async<'T []> = async { 
            let! item = getTable().GetItemAsync(TableKey.Hash hashKey)
            if item.AggregatedUris.Count <> size then
                let msg = sprintf "Result aggregator incomplete (%d/%d)." item.AggregatedUris.Count size
                return! Async.Raise <| new InvalidOperationException(msg)
            else
                return!
                    item.AggregatedUris
                    |> Seq.sortBy (fun e -> e.Key)
                    |> Seq.map (fun e -> S3Persist.ReadPersistedClosure<'T>(clusterId, e.Value))
                    |> Async.Parallel
        }

[<Sealed; AutoSerializable(false)>]
type DynamoDBResultAggregatorFactory private (clusterId : ClusterId) =
    let getTable() = clusterId.GetRuntimeTable<ResultAggregatorEntry>()
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(aggregatorId : string, capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let item = { Id = aggregatorId ; Capacity = capacity ; AggregatedUris = Map.empty }
            let! _ = getTable().PutItemAsync(item, itemDoesNotExist)
            return new ResultAggregator<'T>(clusterId, aggregatorId, capacity) :> ICloudResultAggregator<'T>
        }
    
    static member Create(clusterId : ClusterId) = new DynamoDBResultAggregatorFactory(clusterId)