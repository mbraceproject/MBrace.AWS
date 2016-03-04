namespace MBrace.AWS.Runtime.Utilities

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
    let receiptHandle = message.ReceiptHandle
    let receiveCount = 
        let ok, found = message.Attributes.TryGetValue "ApproximateReceiveCount"
        if ok then int found else 0

    member __.QueueUri = queueUri
    member __.ReceiptHandle = receiptHandle
    member __.Message = message
    member __.ReceiveCount = receiveCount
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


type Sqs =
    static member Enqueue (account : AWSAccount, queueUri : string, msgBody, ?attributes) = async {
        let req = new SendMessageRequest(QueueUrl = queueUri, MessageBody = msgBody)
        attributes |> Option.iter (fun attr -> req.MessageAttributes <- attr)
        let! ct = Async.CancellationToken
        do! account.SQSClient.SendMessageAsync(req, ct) 
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    static member EnqueueBatch (account : AWSAccount, queueUri : string, messages) = async {
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
            do! account.SQSClient.SendMessageBatchAsync(req, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
    }

    static member TryDequeue(account : AWSAccount, queueUri : string, ?messageAttributes, ?visibilityTimeout : int, ?timeoutMilliseconds : int) = async {
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
        let! res = account.SQSClient.ReceiveMessageAsync(req, ct) |> Async.AwaitTaskCorrect

        if res.Messages.Count = 1 then
            let msg = new SqsDequeueMessage(account.SQSClient, queueUri, res.Messages.[0])
            return Some msg
        else 
            return None
    }

    static member DequeueBatch(account : AWSAccount, queueUri : string, ?messageAttributes, ?visibilityTimeout : int, ?maxReceiveCount : int) = async {
        let visibilityTimeout = defaultArg visibilityTimeout 30000 |> float |> TimeSpan.FromMilliseconds
        let req = ReceiveMessageRequest(QueueUrl = queueUri)
        req.MaxNumberOfMessages <- defaultArg maxReceiveCount SqsConstants.maxRecvCount
        req.VisibilityTimeout <- int visibilityTimeout.TotalSeconds
        messageAttributes |> Option.iter (fun ma -> req.MessageAttributeNames <- ma)
        
        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.ReceiveMessageAsync(req, ct) |> Async.AwaitTaskCorrect
        
        return 
            res.Messages 
            |> Seq.map (fun msg -> new SqsDequeueMessage(account.SQSClient, queueUri, msg))
            |> Seq.toArray
    }

    static member DequeueAll(account : AWSAccount, queueUri : string, ?messageAttributes) = async {
        let msgs = ResizeArray<SqsDequeueMessage>()

        // because of the way SQS works, you can get empty receive
        // on a request whilst there are still messages on other
        // hosts, hence we need to tolerate a number of empty
        // receives with small delay in between
        let rec loop numEmptyReceives = async {
            let! batch = Sqs.DequeueBatch(account, queueUri, ?messageAttributes = messageAttributes)
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

    static member DeleteBatch (account : AWSAccount, messages : seq<SqsDequeueMessage>) = async {
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
                let! _response = account.SQSClient.DeleteMessageBatchAsync(request, ct) |> Async.AwaitTaskCorrect
                return ()})
            |> Async.Parallel
            |> Async.Ignore
    }

    static member GetMessageCount (account : AWSAccount, queueUri : string) = async {
        let req = GetQueueAttributesRequest(QueueUrl = queueUri)
        let attrName = QueueAttributeName.ApproximateNumberOfMessages.Value
        req.AttributeNames.Add(attrName)

        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.GetQueueAttributesAsync(req, ct)
                   |> Async.AwaitTaskCorrect

        return res.ApproximateNumberOfMessages
    }

    static member DeleteQueue (account : AWSAccount, queueUri : string) = async {
        let req = DeleteQueueRequest(QueueUrl = queueUri)
        let! ct = Async.CancellationToken
        try 
            do! 
                account.SQSClient.DeleteQueueAsync(req, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore

        with :? QueueDoesNotExistException -> ()
    }

    static member TryGetQueueUri (account : AWSAccount, queueName : string) = async {
        let req  = GetQueueUrlRequest(QueueName = queueName)
        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.GetQueueUrlAsync(req, ct)
                   |> Async.AwaitTaskCorrect
                   |> Async.Catch

        match res with
        | Choice1Of2 res -> return Some res.QueueUrl
        | Choice2Of2 (:? QueueDoesNotExistException) -> return None
        | Choice2Of2 exn -> return! Async.Raise exn
    }

    static member QueueExists (account : AWSAccount, queueName : string) = async {
        let! queueUri = Sqs.TryGetQueueUri (account, queueName)
        return Option.isSome queueUri
    }

    static member CreateQueue (account : AWSAccount, queueName : string) = async {
        try
            let req  = CreateQueueRequest(QueueName = queueName)
            let! ct  = Async.CancellationToken
            let! res =
                account.SQSClient.CreateQueueAsync(req, ct)
                |> Async.AwaitTaskCorrect

            return res.QueueUrl

        with :? QueueDeletedRecentlyException ->
            do! Async.Sleep 1000
            return! Sqs.CreateQueue(account, queueName)
    }

    static member EnsureQueueExists (account : AWSAccount, queueName : string) = async {
        let! queueUri = Sqs.TryGetQueueUri (account, queueName)
        match queueUri with
        | Some queueUri -> return queueUri
        | None -> return! Sqs.CreateQueue (account, queueName)
    }