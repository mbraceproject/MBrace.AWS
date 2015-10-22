namespace MBrace.Azure.Store

open System
open System.Runtime.Serialization

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

/// CloudQueue implementation on top of Amazon SQS
[<AutoSerializable(true) ; Sealed; DataContract>]
type SQSQueue<'T> internal (queueUri, account : AwsSQSAccount) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueueUri")>]
    let queueUri = queueUri

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
                |> Seq.groupBy (fun (i, _) -> i / 10)
                |> Seq.map (fun (_, gr) -> gr |> Seq.map snd)
        
            // TODO: partial failures are not really handled right now
            // TODO: total batch size is not respected here
            for group in groups do
                let req = SendMessageBatchRequest(QueueUrl = queueUri)
                req.Entries.AddRange group
                do! account.SQSClient.SendMessageBatchAsync(req)
                    |> Async.AwaitTaskCorrect
                    |> Async.Ignore
        }
        
        member x.DequeueAsync(timeout) = failwith "Not implemented yet"

        member x.DequeueBatchAsync(maxItems) = failwith "Not implemented yet"
        
        member x.TryDequeueAsync() = failwith "Not implemented yet"

        member x.GetCountAsync() = failwith "Not implemented yet"

        member x.Dispose() = failwith "Not implemented yet"