namespace MBrace.Aws.Runtime.Utilities

open System
open System.IO
open System.Text.RegularExpressions
open System.Threading.Tasks

open MBrace.Aws.Runtime

[<AutoOpen>]
module Utils =
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