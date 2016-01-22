namespace MBrace.AWS.Store

open System
open System.Runtime.Serialization

open Amazon.SQS
open Amazon.SQS.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

/// CloudQueue implementation on top of Amazon SQS
[<AutoSerializable(true) ; Sealed; DataContract>]
type SQSQueue<'T> internal (queueUri, account : AwsAccount) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueueUri")>]
    let queueUri = queueUri
    
    interface CloudQueue<'T> with
        member __.Id = queueUri

        member __.EnqueueAsync(message) = Sqs.enqueue account queueUri (toBase64 message)
        
        member __.EnqueueBatchAsync(messages) = async {
            let msgBodies = messages |> Seq.map toBase64
            do! Sqs.enqueueBatch account queueUri msgBodies
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