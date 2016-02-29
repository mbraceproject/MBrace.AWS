namespace MBrace.AWS.Runtime

open System
open System.Collections.Generic
open System.Runtime.Serialization

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities
open MBrace.AWS

open FSharp.DynamoDB

// Implements an DynamoDB based ICloudCancellationEntry:
// an entity that can be canceled and which supports child entities.
// Used to implement CancellationTokens in MBrace

[<AutoOpen>]
module private CancellationEntryImpl =

    [<ConstantRangeKey("RangeKey", "CancellationToken")>]
    type CancellationEntry =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            IsCancellationRequested : bool

            Children : string list
        }

    let template = RecordTemplate.Define<CancellationEntry>()

    let isNotCancelled = template.PrecomputeConditionalExpr <@ fun c -> c.IsCancellationRequested = false @>
    let cancelOp = template.PrecomputeUpdateExpr <@ fun c -> { c with IsCancellationRequested = true ; Children = [] } @>
    let addChild = template.PrecomputeUpdateExpr <@ fun ch c -> { c with Children = ch :: c.Children } @>

[<Sealed; DataContract>]
type internal DynamoDBCancellationEntry (clusterId : ClusterId, uuid : string) =
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "UUID")>] uuid = uuid

    let getTable() = clusterId.GetRuntimeTable<CancellationEntry>()

    interface ICancellationEntry with        
        member x.UUID: string = uuid

        member x.Cancel(): Async<unit> = async {
            let visited = new HashSet<string>()
            let rec walk id = async {
                if not <| visited.Contains id then
                    let! e = getTable().UpdateItemAsync(TableKey.Hash id, cancelOp, returnLatest = false)
                    if e.IsCancellationRequested then ()
                    else
                        let _ = visited.Add id
                        do! e.Children |> Seq.map walk |> Async.Parallel |> Async.Ignore
            }

            do! walk uuid
        }
        
        member x.Dispose(): Async<unit> = async {
            do! getTable().DeleteItemAsync(TableKey.Hash uuid)
        }
        
        member x.IsCancellationRequested: Async<bool> = async {
            let! record = getTable().GetItemAsync(TableKey.Hash uuid)
            return record.IsCancellationRequested
        }

[<Sealed; AutoSerializable(true)>]
type DynamoDBCancellationTokenFactory private (clusterId : ClusterId) =

    let getTable() = clusterId.GetRuntimeTable<CancellationEntry>()

    interface ICancellationEntryFactory with
        member x.CreateCancellationEntry(): Async<ICancellationEntry> = async {
            let entry = { Id = guid() ; IsCancellationRequested = false ; Children = [] }
            let! _ = getTable().PutItemAsync(entry)
            return new DynamoDBCancellationEntry(clusterId, entry.Id) :> ICancellationEntry
        }
        
        member x.TryCreateLinkedCancellationEntry(parents: ICancellationEntry []): Async<ICancellationEntry option> = async {
            let table = getTable()
            let entry = { Id = guid() ; IsCancellationRequested = false ; Children = [] }
            let updateParent (parent : ICancellationEntry) = async {
                let key = TableKey.Hash parent.UUID
                let! _ = table.UpdateItemAsync(key, addChild entry.Id, precondition = isNotCancelled)
                return ()
            }

            let! _ = table.PutItemAsync entry
            try 
                do! parents |> Seq.map updateParent |> Async.Parallel |> Async.Ignore
                return Some(new DynamoDBCancellationEntry(clusterId, entry.Id) :> _)

            with e when StoreException.PreconditionFailed e -> return None
        }
        
    static member Create(clusterId : ClusterId) = 
        new DynamoDBCancellationTokenFactory(clusterId)