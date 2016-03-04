namespace MBrace.AWS.Runtime.Utilities

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Net
open System.Text.RegularExpressions
open System.Threading.Tasks

open Amazon.Runtime

open MBrace.AWS.Runtime

[<AutoOpen>]
module Utils =

    type internal OAttribute = System.Runtime.InteropServices.OptionalAttribute
    type internal DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

    let guid() = Guid.NewGuid().ToString()

    let toGuid guid = Guid.Parse(guid)
    let fromGuid(guid : Guid) = guid.ToString()

    let uri fmt = Printf.ksprintf (fun s -> new Uri(s)) fmt

    let inline nullable< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > (value : 'T) = 
        new Nullable<'T>(value)

    let inline nullableDefault< 'T when 'T : struct and 'T : (new : unit -> 'T) and  'T :> ValueType > = 
        new Nullable<'T>()

    let (|Null|Nullable|) (value : Nullable<'T>) =
        if value.HasValue then Nullable(value.Value) else Null

    [<RequireQualifiedAccess>]
    module StoreException =
        let checkExn code (e : exn) =
            match e with
            | :? AmazonServiceException as e when e.StatusCode = code -> true
            | :? AggregateException as e ->
                match e.InnerException with
                | :? AmazonServiceException as e when e.StatusCode = code -> true
                | _ -> false
            | _ -> false
    
        let PreconditionFailed (e : exn) = checkExn HttpStatusCode.PreconditionFailed e
        let Conflict (e : exn) = checkExn HttpStatusCode.Conflict e
        let NotFound (e : exn) = checkExn HttpStatusCode.NotFound e

    let doIfNotNull f = function
        | Nullable(x) -> f x
        | _ -> ()

    let fromBase64<'T> (base64 : string) =
        let binary = Convert.FromBase64String base64
        ProcessConfiguration.BinarySerializer.UnPickle<'T>(binary)

    let toBase64 (message : 'T) = 
        message
        |> ProcessConfiguration.BinarySerializer.Pickle 
        |> Convert.ToBase64String

    module Option =
        let ofNullable (x : Nullable<'T>) =
            match x with
            | Nullable x -> Some x
            | _          -> None


    type IDictionary<'K, 'V> with
        member d.TryFind k =
            let ok, found = d.TryGetValue k
            if ok then Some found else None

    type Async with
        static member Cast<'U>(task : Async<obj>) = async { let! t = task in return box t :?> 'U }

    type AsyncOption =
        static member Bind cont p = async {
            let! x = p
            match x with
            | Some x -> return! cont x
            | _      -> return None
        }

        static member Lift (f : 'a -> Async<'b>) = 
            fun x -> async {
                let! res = f x
                return Some res
            }

        static member WithDefault defaultVal p = async {
            let! p = p
            match p with
            | Some x -> return x
            | _      -> return defaultVal
        }

    [<RequireQualifiedAccess>]
    module Array =
        /// <summary>
        ///      Splits input array into chunks of at most chunkSize.
        /// </summary>
        /// <param name="chunkSize">Maximal size used by chunks</param>
        /// <param name="ts">Input array.</param>
        let chunksOf chunkSize (ts : 'T []) =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive integer."
            let chunks = ResizeArray<'T []> ()
            let builder = ResizeArray<'T> ()
            for i = 0 to ts.Length - 1 do
                if builder.Count = chunkSize then
                    chunks.Add (builder.ToArray())
                    builder.Clear()

                builder.Add ts.[i]

            if builder.Count > 0 then
                chunks.Add(builder.ToArray())

            chunks.ToArray()

        let last (ts : 'T []) = ts.[ts.Length - 1]

    module Collection =
        let map (f : 'T -> 'S) (collection : ICollection<'T>) : ICollection<'S> =
            let mapped = Seq.map f collection
            { new ICollection<'S> with
                  member x.Count: int = collection.Count
                  member x.GetEnumerator(): IEnumerator = 
                      (mapped :> IEnumerable).GetEnumerator() : IEnumerator
                  member x.GetEnumerator(): IEnumerator<'S> = 
                      mapped.GetEnumerator()
                  member x.IsReadOnly: bool = true
                  member x.Add(_: 'S): unit = 
                      raise (System.NotSupportedException())
                  member x.Remove(item: 'S): bool = 
                      raise (System.NotSupportedException())
                  member x.Clear(): unit = 
                      raise (System.NotSupportedException())
                  member x.Contains(item: 'S): bool = 
                      raise (System.NotSupportedException())
                  member x.CopyTo(array: 'S [], arrayIndex: int): unit = 
                      raise (System.NotSupportedException())
            }


    [<RequireQualifiedAccess>]
    module Seq =
        /// <summary>
        ///      Splits input sequence into chunks of at most chunkSize.
        /// </summary>
        /// <param name="chunkSize">Maximal size used by chunks</param>
        /// <param name="ts">Input array.</param>
        let chunksOf chunkSize (ts : seq<'T>) : 'T [][] =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive integer."
            let chunks = ResizeArray<'T []> ()
            let builder = ResizeArray<'T> ()
            use e = ts.GetEnumerator()
            while e.MoveNext() do
                if builder.Count = chunkSize then
                    chunks.Add (builder.ToArray())
                    builder.Clear()

                builder.Add e.Current

            if builder.Count > 0 then
                chunks.Add(builder.ToArray())

            chunks.ToArray()