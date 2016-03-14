[<AutoOpen>]
module MBrace.AWS.Runtime.Utilities.SqsUtils

open System
open System.Net
open System.Collections.Generic

open Amazon.SQS
open Amazon.SQS.Model

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.AWS.Runtime

[<RequireQualifiedAccess>]
module private SqsConstants =
    // SQS limits you to 10 messages per batch & total payload size of
    // 256KB (leave 1KB for other attributes, etc.)
    // see http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
    let maxBatchCount   = 10
    let maxBatchPayload = 255 * 1024

    // SQS limits you to 10 messages per receive call
    // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
    let maxRecvCount = 10

    // SQS limits you to up to 20 seconds of long polling wait time
    let maxWaitTime = TimeSpan.FromSeconds 20.

[<Sealed; AutoSerializable(false)>]
type SqsDequeueMessage internal (account : IAmazonSQS, queueUri : string, message : Message) =
    let receiptHandle = message.ReceiptHandle // keep receipt copy since property is settable

    member __.QueueUri = queueUri
    member __.ReceiptHandle = receiptHandle
    member __.Message = message
    member __.RenewLock(?timeoutMilliseconds : int) = async {
        let timeout = defaultArg timeoutMilliseconds 30000 |> float |> TimeSpan.FromMilliseconds
        let request = new ChangeMessageVisibilityRequest(queueUri, receiptHandle, int timeout.TotalSeconds)
        let! ct = Async.CancellationToken
        let! response = account.ChangeMessageVisibilityAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwith "failed to update renew lock."

        return ()
    }

    member __.Complete() = async {
        let request = new DeleteMessageRequest(queueUri, receiptHandle)
        let! ct = Async.CancellationToken
        let! response = account.DeleteMessageAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwith "failed to delete message."

        return ()
    }

    member __.Abandon() = __.RenewLock(timeoutMilliseconds = 0)


type IAmazonSQS with
    member client.Enqueue (queueUri : string, msgBody : string, ?attributes) = async {
        let req = new SendMessageRequest(QueueUrl = queueUri, MessageBody = msgBody)
        attributes |> Option.iter (fun attr -> req.MessageAttributes <- attr)
        let! ct = Async.CancellationToken
        do! client.SendMessageAsync(req, ct) 
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    member client.EnqueueBatch (queueUri : string, messages) = async {
        let groups = messages |> Seq.chunksOf SqsConstants.maxBatchCount
        
        // TODO: partial failures are not handled right now
        // TODO: total batch payload size is not respected here
        for group in groups do
            let req = SendMessageBatchRequest(QueueUrl = queueUri)
            group |> Seq.iteri (fun i (body,attrs) -> 
                let e = new SendMessageBatchRequestEntry(string i, body)
                attrs |> Option.iter (fun a -> e.MessageAttributes <- a)
                req.Entries.Add e)

            let! ct = Async.CancellationToken
            do! client.SendMessageBatchAsync(req, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
    }

    member client.TryDequeue(queueUri : string, ?messageAttributes, ?visibilityTimeout : int, ?timeoutMilliseconds : int) = async {
        let timeout = defaultArg timeoutMilliseconds 20000 |> float |> TimeSpan.FromMilliseconds
        let visibilityTimeout = defaultArg visibilityTimeout 30000 |> float |> TimeSpan.FromMilliseconds
        if timeout > SqsConstants.maxWaitTime then invalidArg "timeoutMilliseconds" "must be at most 20 seconds"
        let req = ReceiveMessageRequest(QueueUrl = queueUri)
        messageAttributes |> Option.iter (fun ma -> req.MessageAttributeNames <- ma)
        req.MaxNumberOfMessages <- 1
        req.VisibilityTimeout <- int visibilityTimeout.TotalSeconds
        
        // always make use of long polling for efficiency
        // but never wait for more than max allowed (20 seconds)
        req.WaitTimeSeconds <- int timeout.TotalSeconds

        let! ct  = Async.CancellationToken
        let! res = client.ReceiveMessageAsync(req, ct) |> Async.AwaitTaskCorrect

        if res.Messages.Count = 1 then
            let msg = new SqsDequeueMessage(client, queueUri, res.Messages.[0])
            return Some msg
        else 
            return None
    }

    member client.DequeueBatch(queueUri : string, ?messageAttributes, ?visibilityTimeout : int, ?maxReceiveCount : int) = async {
        let visibilityTimeout = defaultArg visibilityTimeout 30000 |> float |> TimeSpan.FromMilliseconds
        let req = ReceiveMessageRequest(QueueUrl = queueUri)
        req.MaxNumberOfMessages <- defaultArg maxReceiveCount SqsConstants.maxRecvCount
        req.VisibilityTimeout <- int visibilityTimeout.TotalSeconds
        messageAttributes |> Option.iter (fun ma -> req.MessageAttributeNames <- ma)
        
        let! ct  = Async.CancellationToken
        let! res = client.ReceiveMessageAsync(req, ct) |> Async.AwaitTaskCorrect
        
        return 
            res.Messages 
            |> Seq.map (fun msg -> new SqsDequeueMessage(client, queueUri, msg))
            |> Seq.toArray
    }

    member client.DequeueAll(queueUri : string, ?messageAttributes, ?visibilityTimeout : int) = async {
        let msgs = ResizeArray<SqsDequeueMessage>()

        // because of the way SQS works, you can get empty receive
        // on a request whilst there are still messages on other
        // hosts, hence we need to tolerate a number of empty
        // receives with small delay in between
        let rec loop numEmptyReceives = async {
            let! batch = client.DequeueBatch(queueUri,
                            ?visibilityTimeout = visibilityTimeout,
                            ?messageAttributes = messageAttributes)

            match batch with
            | [||] when numEmptyReceives < 10 -> 
                do! Async.Sleep 100
                return! loop (numEmptyReceives + 1)
            | [||] -> return ()
            | _    -> 
                msgs.AddRange batch
                do! loop 0
        }

        do! loop 0
        return msgs.ToArray()
    }

    member client.DeleteBatch (messages : seq<SqsDequeueMessage>) = async {
        do!
            messages 
            |> Seq.chunksOf SqsConstants.maxBatchCount
            |> Seq.map (fun group -> async {
                let queueUri = group.[0].QueueUri
                let request = new DeleteMessageBatchRequest(QueueUrl = queueUri)
                for i = 0 to group.Length - 1 do
                    let e = DeleteMessageBatchRequestEntry(string i, group.[i].ReceiptHandle)
                    request.Entries.Add e

                let! ct = Async.CancellationToken
                let! _response = client.DeleteMessageBatchAsync(request, ct) |> Async.AwaitTaskCorrect
                return ()})
            |> Async.Parallel
            |> Async.Ignore
    }

    member client.GetMessageCount (queueUri : string) = async {
        let req = GetQueueAttributesRequest(QueueUrl = queueUri)
        let attrName = QueueAttributeName.ApproximateNumberOfMessages.Value
        req.AttributeNames.Add(attrName)

        let! ct  = Async.CancellationToken
        let! res = client.GetQueueAttributesAsync(req, ct) |> Async.AwaitTaskCorrect
        return res.ApproximateNumberOfMessages
    }

    member client.TryGetQueueUri (queueName : string) = async {
        let req  = GetQueueUrlRequest(QueueName = queueName)
        let! ct  = Async.CancellationToken
        let! res = client.GetQueueUrlAsync(req, ct)
                   |> Async.AwaitTaskCorrect
                   |> Async.Catch

        match res with
        | Choice1Of2 res -> return Some res.QueueUrl
        | Choice2Of2 (:? QueueDoesNotExistException) -> return None
        | Choice2Of2 exn -> return! Async.Raise exn
    }

    member client.GetQueueUris(?prefix : string) = async {
        let req = ListQueuesRequest()
        prefix |> Option.iter (fun p -> req.QueueNamePrefix <- p)
        let! ct = Async.CancellationToken
        let! res = client.ListQueuesAsync(req, ct) |> Async.AwaitTaskCorrect
        return res.QueueUrls
    }

    member client.DeleteQueueUri (queueUri : string) = async {
        let req = DeleteQueueRequest(QueueUrl = queueUri)
        let! ct = Async.CancellationToken
        try 
            do! 
                client.DeleteQueueAsync(req, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore

        with :? QueueDoesNotExistException -> ()
    }

    member client.QueueNameExists (queueName : string) = async {
        let! queueUri = client.TryGetQueueUri queueName
        return Option.isSome queueUri
    }

    member client.CreateQueueWithName (queueName : string) = async {
        try
            let req  = CreateQueueRequest(QueueName = queueName)
            let! ct  = Async.CancellationToken
            let! res =
                client.CreateQueueAsync(req, ct)
                |> Async.AwaitTaskCorrect

            return res.QueueUrl

        with :? QueueDeletedRecentlyException ->
            do! Async.Sleep 1000
            return! client.CreateQueueWithName queueName
    }

    member client.EnsureQueueNameExists (queueName : string) = async {
        let! queueUri = client.TryGetQueueUri queueName
        match queueUri with
        | Some queueUri -> return true, queueUri
        | None -> 
            let! queueUri = client.CreateQueueWithName queueName
            return false, queueUri
    }