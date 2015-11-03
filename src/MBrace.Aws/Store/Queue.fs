namespace MBrace.Aws.Store

open System
open System.Runtime.Serialization

open Amazon.SQS
open Amazon.SQS.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.Aws.Runtime
open MBrace.Aws.Runtime.Utilities

/// CloudQueue implementation on top of Amazon SQS
[<AutoSerializable(true) ; Sealed; DataContract>]
type SQSQueue<'T> internal (queueUri, account : AwsSQSAccount) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueueUri")>]
    let queueUri = queueUri

    let toBatchEntry (message : 'T) =
        SendMessageBatchRequestEntry(
            Id = Guid.NewGuid().ToString(),
            MessageBody = toBase64 message)

    interface CloudQueue<'T> with
        member __.Id = queueUri

        member __.EnqueueAsync(message) = Sqs.enqueue account queueUri (toBase64 message)
        
        member __.EnqueueBatchAsync(messages) = async {
            let entries = messages |> Seq.map toBatchEntry
            let groups = 
                entries 
                |> Seq.mapi (fun i e -> i, e)
                |> Seq.groupBy (fun (i, _) -> i / SqsConstants.maxBatchCount)
                |> Seq.map (fun (_, gr) -> gr |> Seq.map snd)
        
            // TODO: partial failures are not handled right now
            // TODO: total batch payload size is not respected here
            for group in groups do
                let req = SendMessageBatchRequest(QueueUrl = queueUri)
                req.Entries.AddRange group
                let! ct = Async.CancellationToken
                do! account.SQSClient.SendMessageBatchAsync(req, ct)
                    |> Async.AwaitTaskCorrect
                    |> Async.Ignore
        }
        
        member __.DequeueAsync(timeout) = async {
            match timeout with
            | Some _ ->
                let! msg = Sqs.dequeue account queueUri timeout
                match msg with
                | Some body -> return fromBase64<'T> body
                | _         -> return! Async.Raise(TimeoutException())
            | _ -> 
                let rec aux _ = async {
                    let! msg = Sqs.dequeue account queueUri timeout
                    match msg with
                    | Some body -> return fromBase64<'T> body
                    | _ -> return! aux()
                }
                return! aux ()
        }

        member __.DequeueBatchAsync(maxItems) = async {
            let req = ReceiveMessageRequest(QueueUrl = queueUri)
            req.MaxNumberOfMessages <- min maxItems SqsConstants.maxRecvCount

            let! ct = Async.CancellationToken
            let! res = account.SQSClient.ReceiveMessageAsync(req, ct)
                       |> Async.AwaitTaskCorrect
            return res.Messages 
                   |> Seq.map (fun msg -> fromBase64<'T> msg.Body) 
                   |> Seq.toArray
        }
        
        member __.TryDequeueAsync() = async {
            let! body = Sqs.dequeue account queueUri None
            match body with
            | Some x -> return Some <| fromBase64<'T> x
            | _ -> return None
        }

        member x.GetCountAsync() = Sqs.getCount account queueUri

        member x.Dispose() = Sqs.deleteQueue account queueUri