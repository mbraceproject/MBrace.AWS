namespace MBrace.AWS.Runtime

open System
open System.Collections.Concurrent
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
    
    // placeholder value which avoids using empty list representations in Dynamo
    let private placeHolder = "_empty"

    [<ConstantRangeKey("RangeKey", "CancellationToken")>]
    type CancellationEntry =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            IsCancellationRequested : bool

            Children : string list
        }
    with
        static member CreateNew() =
            let id = "cancellationToken:" + guid()
            { Id = id ; IsCancellationRequested = false ; Children = [placeHolder] }

        member __.Children' =
            __.Children |> Seq.filter (fun ch -> ch <> placeHolder)

    let private template = RecordTemplate.Define<CancellationEntry>()

    let isNotCancelled = template.PrecomputeConditionalExpr <@ fun c -> c.IsCancellationRequested = false @>
    let cancelOp = template.PrecomputeUpdateExpr <@ fun c -> { c with IsCancellationRequested = true } @>
    let addChild = template.PrecomputeUpdateExpr <@ fun ch c -> { c with Children = ch :: c.Children } @>

[<Sealed; DataContract>]
type internal DynamoDBCancellationEntry (clusterId : ClusterId, uuid : string) =
    let [<DataMember(Name = "ClusterId")>] clusterId = clusterId
    let [<DataMember(Name = "UUID")>] uuid = uuid

    let getTable() = clusterId.GetRuntimeTable<CancellationEntry>()

    interface ICancellationEntry with        
        member x.UUID: string = uuid

        member x.Cancel(): Async<unit> = async {
            let table = getTable()
            let visited = new ConcurrentDictionary<string, unit>()
            let rec walk id = async {
                if visited.TryAdd(id, ()) then
                    let! e = table.UpdateItemAsync(TableKey.Hash id, cancelOp, returnLatest = false)
                    if e.IsCancellationRequested then ()
                    else
                        do! e.Children' |> Seq.map walk |> Async.Parallel |> Async.Ignore
            }

            do! walk uuid
        }
        
        member x.Dispose(): Async<unit> = async {
            let! _ = getTable().DeleteItemAsync(TableKey.Hash uuid)
            return ()
        }
        
        member x.IsCancellationRequested: Async<bool> = async {
            let! record = getTable().GetItemAsync(TableKey.Hash uuid)
            return record.IsCancellationRequested
        }


type internal DynamoDBCancellationToken =
    static member ToUUID(token : CloudCancellationToken) =
        let _ = token.ElevateToGlobal()
        token.GlobalId

    static member FromUUID(clusterId : ClusterId, uuid : string option) =
        match uuid with
        | None -> CloudCancellationToken.Canceled
        | Some uuid ->
            let entry = new DynamoDBCancellationEntry(clusterId, uuid)
            CloudCancellationToken.FromEntry entry
        

[<Sealed; AutoSerializable(true)>]
type DynamoDBCancellationTokenFactory private (clusterId : ClusterId) =

    let getTable() = clusterId.GetRuntimeTable<CancellationEntry>()

    interface ICancellationEntryFactory with

        member x.TryCreateCancellationEntry(parents: ICancellationEntry []): Async<ICancellationEntry option> = async {
            let table = getTable()
            let entry = CancellationEntry.CreateNew()
            let updateParent (parent : ICancellationEntry) = async {
                let key = TableKey.Hash parent.UUID
                let expr = addChild entry.Id
                let! _ = table.UpdateItemAsync(key, expr, precondition = isNotCancelled)
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