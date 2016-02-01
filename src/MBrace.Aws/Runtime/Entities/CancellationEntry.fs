namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Runtime.Serialization

open MBrace.Runtime
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open Amazon.DynamoDBv2.DocumentModel

// Implements an DynamoDB based ICloudCancellationEntry:
// an entity that can be canceled and which supports child entities.
// Used to implement CancellationTokens in MBrace

type CancellationTokenSourceEntity(uuid : string, isCancellationRequested : bool, children : string list) =
    inherit DynamoDBTableEntity(CancellationTokenSourceEntity.DefaultHashKey, uuid)

    do if children.Length > CancellationTokenSourceEntity.MaxChildren then 
        raise <| ArgumentOutOfRangeException("Number of cancellation entry children exceeds maximum permitted.")

    member __.Id = uuid
    member __.IsCancellationRequested = isCancellationRequested
    member __.Children = children

    static member DefaultHashKey = "cancellationToken"
    static member MaxChildren = 4096

    interface IDynamoDBDocument with
        member this.ToDynamoDBDocument () =
            let doc = new Document()

            doc.[HashKey]  <- DynamoDBEntry.op_Implicit(this.HashKey)
            doc.[RangeKey] <- DynamoDBEntry.op_Implicit(this.RangeKey)
            doc.["IsCancellationRequested"]  <- DynamoDBEntry.op_Implicit(this.IsCancellationRequested)
            doc.["Children"] <- DynamoDBEntry.op_Implicit(new ResizeArray<_>(this.Children))

            doc

    static member FromDynamoDBDocument (doc : Document) = 
        let uuid = doc.[RangeKey].AsString()
        let isCancellationRequested = doc.["IsCancellationRequested"].AsBoolean()
        let children = doc.["Children"].AsListOfString() |> Seq.toList

        new CancellationTokenSourceEntity(uuid, isCancellationRequested, children)


[<Sealed; DataContract>]
type internal DynamoDBCancellationEntry (clusterId : ClusterId, uuid : string) =
    let [<DataMember(Name = "ClusterId")>] id = clusterId
    let [<DataMember(Name = "UUID")>] uuid = uuid

    interface ICancellationEntry with        
        member x.UUID: string = uuid

        member x.Cancel(): Async<unit> = async {
            let visited = new HashSet<string>()
            let rec walk rowKey = async {
                if not <| visited.Contains rowKey then
                    let! e = Table.read<CancellationTokenSourceEntity> id.DynamoDBAccount id.RuntimeTable CancellationTokenSourceEntity.DefaultHashKey rowKey
                    if e.IsCancellationRequested then ()
                    else
                        let _ = visited.Add rowKey
                        for e' in e.Children do do! walk e'
            }

            do! walk uuid
        
            do! visited
                |> Seq.map (fun rowKey -> new CancellationTokenSourceEntity(rowKey, isCancellationRequested = true, children = []))
                |> Table.putBatch id.DynamoDBAccount id.RuntimeTable
        }
        
        member x.Dispose(): Async<unit> = async {
            do! Table.delete id.DynamoDBAccount id.RuntimeTable (new CancellationTokenSourceEntity(uuid, false, []))
        }
        
        member x.IsCancellationRequested: Async<bool> = async {
            let! record = Table.read<CancellationTokenSourceEntity> id.DynamoDBAccount id.RuntimeTable CancellationTokenSourceEntity.DefaultHashKey uuid
            return record.IsCancellationRequested
        }

[<Sealed>]
type DynamoDBCancellationTokenFactory private (clusterId : ClusterId) =
    interface ICancellationEntryFactory with
        member x.CreateCancellationEntry(): Async<ICancellationEntry> = async {
            let record = new CancellationTokenSourceEntity(guid(), false, [])
            let! _record = Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable record
            return new DynamoDBCancellationEntry(clusterId, record.Id) :> ICancellationEntry
        }
        
        member x.TryCreateLinkedCancellationEntry(parents: ICancellationEntry []): Async<ICancellationEntry option> = async {
            let uuid = guid()
            let record = new CancellationTokenSourceEntity(uuid, false, [])

            let rec loop () = async {
                let! parents = 
                    parents 
                    |> Seq.map (fun p -> Table.read<CancellationTokenSourceEntity> clusterId.DynamoDBAccount clusterId.RuntimeTable CancellationTokenSourceEntity.DefaultHashKey p.UUID)
                    |> Async.Parallel

                if parents |> Array.exists (fun p -> p.IsCancellationRequested) then
                    return None
                else
                    return raise <| new NotImplementedException("need conditional updates")
//                    insert record
//                    for parent in parents do
//                        let newParent = new CancellationTokenSourceEntity(parent.RangeKey, false, uuid :: parent.Children)
//                        newParent.ETag <- parent.ETag
//                        tbo.Merge(newParent)
//                    try
//                        do! Table.batch clusterId.StorageAccount clusterId.RuntimeTable tbo
//                        return Some(TableCancellationEntry(clusterId, uuid) :> ICancellationEntry)
//                    with ex when StoreException.PreconditionFailed ex ->
//                        return! loop ()
            }

            return! loop ()
        }
        
    static member Create(clusterId : ClusterId) = 
        new DynamoDBCancellationTokenFactory(clusterId)