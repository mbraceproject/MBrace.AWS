namespace MBrace.Azure.Store

open System
open System.Runtime.Serialization

open Amazon.SQS
open Amazon.SQS.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.Aws.Runtime

[<AutoOpen>]
module private SQSQueueUtils =
    // SQS limits you to 10 messages per batch & total payload size of
    // 256KB (leave 1KB for other attributes, etc.)
    // see http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
    let maxBatchCount   = 10
    let maxBatchPayload = 255 * 1024

    // SQS limits you to 10 messages per receive call
    // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
    let maxRecvCount    = 10

    // SQS limits you to up to 20 seconds of long polling wait time
    let maxWaitTime = 20 * 1000 // in milliseconds

/// CloudQueue implementation on top of Amazon SQS
[<AutoSerializable(true) ; Sealed; DataContract>]
type SQSQueue<'T> internal (queueUri, account : AwsSQSAccount) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueueUri")>]
    let queueUri = queueUri

    let fromBase64 (base64 : string) =
        let binary = Convert.FromBase64String base64
        ProcessConfiguration.BinarySerializer.UnPickle<'T>(binary)

    let readBody (msg : Message) = fromBase64 msg.Body

    let toBase64 (message : 'T) = 
        message
        |> ProcessConfiguration.BinarySerializer.Pickle 
        |> Convert.ToBase64String

    let toBatchEntry (message : 'T) =
        SendMessageBatchRequestEntry(
            Id = Guid.NewGuid().ToString(),
            MessageBody = toBase64 message)

    interface CloudQueue<'T> with
        member __.Id = queueUri

        member __.EnqueueAsync(message) = async {
            let req = SendMessageRequest(
                        QueueUrl = queueUri, 
                        MessageBody = toBase64 message)
            do! account.SQSClient.SendMessageAsync(req) 
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }
        
        member __.EnqueueBatchAsync(messages) = async {
            let entries = messages |> Seq.map toBatchEntry
            let groups = 
                entries 
                |> Seq.mapi (fun i e -> i, e)
                |> Seq.groupBy (fun (i, _) -> i / maxBatchCount)
                |> Seq.map (fun (_, gr) -> gr |> Seq.map snd)
        
            // TODO: partial failures are not handled right now
            // TODO: total batch payload size is not respected here
            for group in groups do
                let req = SendMessageBatchRequest(QueueUrl = queueUri)
                req.Entries.AddRange group
                do! account.SQSClient.SendMessageBatchAsync(req)
                    |> Async.AwaitTaskCorrect
                    |> Async.Ignore
        }
        
        member __.DequeueAsync(timeout) = async {
            let req = ReceiveMessageRequest(QueueUrl = queueUri)
            req.MaxNumberOfMessages <- 1
            
            // always make use of long polling for efficiency
            // but never wait for more than max allowed (20 seconds)
            req.WaitTimeSeconds <- min maxWaitTime <| defaultArg timeout maxWaitTime

            match timeout with
            | Some _ ->
                let! res = account.SQSClient.ReceiveMessageAsync(req)
                           |> Async.AwaitTaskCorrect
                if res.Messages.Count = 1
                then return readBody res.Messages.[0]
                else return! Async.Raise(TimeoutException())
            | _ -> 
                let rec aux _ = async {
                    let! res = account.SQSClient.ReceiveMessageAsync(req)
                               |> Async.AwaitTaskCorrect
                    if res.Messages.Count = 1
                    then return readBody res.Messages.[0]
                    else return! aux ()
                }
                return! aux ()
        }

        member __.DequeueBatchAsync(maxItems) = async {
            let req = ReceiveMessageRequest(QueueUrl = queueUri)
            req.MaxNumberOfMessages <- min maxItems maxRecvCount

            let! res = account.SQSClient.ReceiveMessageAsync(req)
                       |> Async.AwaitTaskCorrect
            return res.Messages |> Seq.map readBody |> Seq.toArray
        }
        
        member __.TryDequeueAsync() = async {
            let req = ReceiveMessageRequest(QueueUrl = queueUri)
            req.MaxNumberOfMessages <- 1

            let! res = account.SQSClient.ReceiveMessageAsync(req)
                       |> Async.AwaitTaskCorrect
            if res.Messages.Count = 1 
            then return Some <| readBody res.Messages.[0]
            else return None
        }

        member x.GetCountAsync() = async {
            let req = GetQueueAttributesRequest(QueueUrl = queueUri)
            let attrName = QueueAttributeName.ApproximateNumberOfMessages.Value
            req.AttributeNames.Add(attrName)

            let! res = account.SQSClient.GetQueueAttributesAsync(req)
                       |> Async.AwaitTaskCorrect
            return int64 res.ApproximateNumberOfMessages
        }

        member x.Dispose() = failwith "Not implemented yet"