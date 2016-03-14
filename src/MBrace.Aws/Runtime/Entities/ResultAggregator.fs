namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text.RegularExpressions

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open FSharp.DynamoDB

 [<AutoOpen>]
 module private ResultAggregatorImpl =

    [<Literal>]
    let private placeHolder = "head"

    let toKey (index:int) = sprintf "item%d" index
    let private kr = new Regex("item([0-9]+)", RegexOptions.Compiled)
    let fromKey (key:string) = 
        let m = kr.Match(key)
        if m.Success then m.Groups.[1].Value |> int
        else invalidOp <| sprintf "invalid ResultAggregator key '%s'" key

    [<ConstantRangeKey("RangeKey", "ResultAggregator")>]
    type ResultAggregatorEntry =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            Capacity : int

            AggregatedUris : Map<string, string>
        }
    with
        static member Init(id, capacity) =
            { Id = id ; Capacity = capacity ; AggregatedUris = Map.ofList [(placeHolder, placeHolder)] }

        member __.Count = __.AggregatedUris.Count - 1
        member __.Uris =
            __.AggregatedUris
            |> Seq.filter (fun i -> i.Key <> placeHolder)
            |> Seq.sortBy (fun i -> fromKey i.Key)
            |> Seq.map (fun i -> i.Value)
        

    let private template = RecordTemplate.Define<ResultAggregatorEntry>()

    let entryNotExists = 
        <@ fun id r -> r.AggregatedUris |> Map.containsKey id |> not @>
        |> template.PrecomputeConditionalExpr

    let addEntry =
        <@ fun id path r -> { r with AggregatedUris = r.AggregatedUris |> Map.add id path } @>
        |> template.PrecomputeUpdateExpr

[<DataContract; Sealed>]
type ResultAggregator<'T> internal (clusterId : ClusterId, hashKey : string, size : int) =
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "HashKey")>] hashKey = hashKey
    let [<DataMember(Name = "Size")>] size = size

    let getTable() = clusterId.GetRuntimeTable<ResultAggregatorEntry>()

    member this.UUID = hashKey

    interface ICloudResultAggregator<'T> with
        member this.Capacity: int = size
        
        member this.CurrentSize: Async<int> = async {
            let! item = getTable().GetItemAsync(TableKey.Hash hashKey)
            return item.Count
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
            let uri = sprintf "%s/%s" hashKey (guid())
            do! S3Persist.PersistClosure(clusterId, value, uri, allowNewSifts = false)
            let id = toKey index
            let! item = getTable().UpdateItemAsync(TableKey.Hash hashKey, 
                                                    addEntry id uri, 
                                                    precondition = entryNotExists id)

            return item.Count = size
        }
        
        member this.ToArray(): Async<'T []> = async { 
            let! item = getTable().GetItemAsync(TableKey.Hash hashKey)
            if item.Count < size then
                let msg = sprintf "Result aggregator incomplete (%d/%d)." item.AggregatedUris.Count size
                return! Async.Raise <| new InvalidOperationException(msg)
            else
                return!
                    item.Uris
                    |> Seq.map (fun u -> S3Persist.ReadPersistedClosure<'T>(clusterId, u))
                    |> Async.Parallel
        }

[<Sealed; AutoSerializable(false)>]
type DynamoDBResultAggregatorFactory private (clusterId : ClusterId) =
    let getTable() = clusterId.GetRuntimeTable<ResultAggregatorEntry>()
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(aggregatorId : string, capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let id = "resultAggregator:" + aggregatorId
            let item = ResultAggregatorEntry.Init(id, capacity)
            let! _ = getTable().PutItemAsync(item, itemDoesNotExist)
            return new ResultAggregator<'T>(clusterId, id, capacity) :> ICloudResultAggregator<'T>
        }
    
    static member Create(clusterId : ClusterId) = new DynamoDBResultAggregatorFactory(clusterId)