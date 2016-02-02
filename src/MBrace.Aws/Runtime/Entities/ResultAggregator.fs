namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Runtime.Serialization

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open Amazon.DynamoDBv2.DocumentModel

(*
 * A ResultAggregator of length N consists of N indexed entries and 1 guard entry.
 * All entries share the ResultAggregators partitionKey, each one of the indexed use
 * its index as a rowKey, the guard entry uses an empty rowKey and acts like a counter.
 * Each update changes both the indexed entry and the control entry in a single batch operation.
 *
 *  +-------------+--------------+---------------+---------------+
 *    PartitionKey     RowKey         Uri             Counter
 *  +-------------+--------------+---------------+---------------+
 *     aggr_pk          empty            -                1
 *  +-------------+--------------+---------------+---------------+
 *     aggr_pk            0           empty              -
 *     aggr_pk            1           someUri            -
 *  +-------------+--------------+---------------+---------------+
 *
 *)

 type IndexedReferenceEntity(hashKey, rangeKey, uri : string, workerId : string, counter : int, ?etag : string) = 
    inherit DynamoDBTableEntity(hashKey, rangeKey)
    member __.S3Uri = uri
    member __.WorkerId = workerId
    member __.Counter = counter
    member __.ETag = etag

    member this.Incr() = new IndexedReferenceEntity(this.HashKey, this.RangeKey, this.S3Uri, this.WorkerId, this.Counter + 1, ?etag = this.ETag)

    static member MakeRangeKey(index) = sprintf "%010d" index
    static member DefaultRangeKey = ""

    interface IDynamoDBDocument with
        member this.ToDynamoDBDocument() =
            let doc = new Document()
            doc.[HashKey] <- DynamoDBEntry.op_Implicit(this.HashKey)
            doc.[RangeKey] <- DynamoDBEntry.op_Implicit(this.RangeKey)
            Table.writeETag doc this.ETag
            doc.["S3Uri"] <- DynamoDBEntry.op_Implicit(this.S3Uri)
            doc.["WorkerId"] <- DynamoDBEntry.op_Implicit(this.WorkerId)
            doc.["Counter"] <- DynamoDBEntry.op_Implicit(counter)
            doc

    static member FromDynamoDBDocument (doc : Document) =
        let hashKey = doc.[HashKey].AsString()
        let rangeKey = doc.[RangeKey].AsString()
        let etag = Table.readETag doc
        let uri = doc.["S3Uri"].AsString()
        let workerId = doc.["WorkerId"].AsString()
        let counter = doc.["Counter"].AsInt()
        new IndexedReferenceEntity(hashKey, rangeKey, uri, workerId, counter, etag)

[<DataContract; Sealed>]
type ResultAggregator<'T> internal (clusterId : ClusterId, hashKey : string, size : int) =
    static let enableOverWrite = false
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "HashKey")>] partitionKey = hashKey
    let [<DataMember(Name = "Size")>] size = size

    let getEntities () = async {
        return! Table.query<IndexedReferenceEntity> clusterId.DynamoDBAccount clusterId.RuntimeTable hashKey
    }

    let getGuard (entities : seq<IndexedReferenceEntity>) = 
        entities |> Seq.find (fun e -> e.RangeKey = IndexedReferenceEntity.DefaultRangeKey)

    let tryGetEntity (index : int) (entities : seq<IndexedReferenceEntity>) = 
        entities |> Seq.tryFind (fun e -> e.RangeKey = IndexedReferenceEntity.MakeRangeKey index)

    member this.UUID = hashKey

    interface ICloudResultAggregator<'T> with
        member this.Capacity: int = size
        
        member this.CurrentSize: Async<int> = async {
            let! entities = getEntities ()
            let guard = getGuard entities
            return guard.Counter
        }
        
        member this.Dispose(): Async<unit> = async {
            let! records = getEntities ()
            do! Table.deleteBatch clusterId.DynamoDBAccount clusterId.RuntimeTable records
            do! 
                records
                |> Seq.choose (fun r -> match r.S3Uri with null -> None | uri -> Some uri)
                |> Seq.map (fun uri -> S3Persist.DeletePersistedClosure(clusterId, uri))
                |> Async.Parallel
                |> Async.Ignore
        }
        
        member this.IsCompleted: Async<bool> = async {
            let! currentSize = (this :> ICloudResultAggregator<'T>).CurrentSize
            return currentSize = size
        }
        
        member this.SetResult(index: int, value: 'T, workerId : IWorkerId): Async<bool> = async { 
            let rangeKey = IndexedReferenceEntity.MakeRangeKey index
            let uri = sprintf "%s/%s" partitionKey (guid())
            do! S3Persist.PersistClosure(clusterId, value, uri, allowNewSifts = false)
            let entity = new IndexedReferenceEntity(hashKey, rangeKey, uri, workerId.Id, -1)
            let rec loop () = async {
                let! entities = getEntities()
                let guard = getGuard entities
                match tryGetEntity index entities with
                | Some index when index.S3Uri <> uri && enableOverWrite = false -> return guard.Counter = size
                | entOpt ->
                    try
                        if Option.isNone entOpt then
                            do! Table.update clusterId.DynamoDBAccount clusterId.RuntimeTable entity

                        let guard' = guard.Incr()
                        do! Table.update clusterId.DynamoDBAccount clusterId.RuntimeTable guard'
                        return guard.Counter = size
                    with e when StoreException.PreconditionFailed e ->
                        return! loop ()
            }

            return! loop ()
        }
        
        member this.ToArray(): Async<'T []> = async { 
            let! entities = getEntities()
            let guard = getGuard entities
            if guard.Counter <> size then
                return! Async.Raise <| new InvalidOperationException(sprintf "Result aggregator incomplete (%d/%d)." guard.Counter size)
            else
                return!
                    entities
                    |> Seq.filter (fun e -> e.S3Uri <> null) 
                    |> Seq.sortBy (fun r -> r.RangeKey)
                    |> Seq.map (fun e -> S3Persist.ReadPersistedClosure<'T>(clusterId, e.S3Uri))
                    |> Async.Parallel
        }

[<Sealed; AutoSerializable(false)>]
type DynamoDBResultAggregatorFactory private (clusterId : ClusterId) =
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(_aggregatorId : string, capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let partitionKey = guid()
            let guard = new IndexedReferenceEntity(partitionKey, IndexedReferenceEntity.DefaultRangeKey, null, null, counter = 0)
            do! Table.put clusterId.S3Account clusterId.RuntimeTable guard
            return new ResultAggregator<'T>(clusterId, partitionKey, capacity) :> ICloudResultAggregator<'T>
        }
    
    static member Create(clusterId : ClusterId) = new DynamoDBResultAggregatorFactory(clusterId)