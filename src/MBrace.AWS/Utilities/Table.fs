namespace MBrace.AWS.Runtime.Utilities

open System
open System.Collections.Generic

open Nessos.FsPickler

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

open MBrace.Core.Internals
open MBrace.Runtime.Utils.Retry
open MBrace.AWS.Runtime

/// Serialize property to DynamoDB using FsPickler binary serializer
type FsPicklerBinaryAttribute() =
    inherit PropertySerializerAttribute<byte[]> ()
    override __.Serialize value = 
        ProcessConfiguration.BinarySerializer.Pickle value
    override __.Deserialize pickle =
        ProcessConfiguration.BinarySerializer.UnPickle<'T> pickle

/// Serialize property to DynamoDB using FsPickler Json serializer
type FsPicklerJsonAttribute() =
    inherit PropertySerializerAttribute<string> ()
    override __.Serialize value = 
        ProcessConfiguration.JsonSerializer.PickleToString value
    override __.Deserialize pickle =
        ProcessConfiguration.JsonSerializer.UnPickleOfString<'T> pickle

[<AutoOpen>]
module internal TableUtils =

    type IAmazonDynamoDB with
        member ddb.DeleteTableAsyncSafe(tableName : string) = async {
            try
                let! ct = Async.CancellationToken
                let! _response = ddb.DeleteTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
                return ()
            with 
            | :? Amazon.DynamoDBv2.Model.ResourceNotFoundException -> return ()
            | :? Amazon.DynamoDBv2.Model.ResourceInUseException ->
                do! Async.Sleep 1000
                do! ddb.DeleteTableAsyncSafe(tableName)
        }