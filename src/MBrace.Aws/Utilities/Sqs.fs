namespace MBrace.Aws.Runtime.Utilities

open System
open System.Collections.Generic

open Amazon.SQS
open Amazon.SQS.Model

open Nessos.FsPickler

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<RequireQualifiedAccess>]
module SqsConstants =
    // SQS limits you to 10 messages per batch & total payload size of
    // 256KB (leave 1KB for other attributes, etc.)
    // see http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
    let maxBatchCount   = 10
    let maxBatchPayload = 255 * 1024

    // SQS limits you to 10 messages per receive call
    // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
    let maxRecvCount = 10

    // SQS limits you to up to 20 seconds of long polling wait time
    let maxWaitTime = 20 * 1000 // in milliseconds

[<RequireQualifiedAccess>]
module Sqs =
    let enqueue (account : AwsSQSAccount) queueUri msgBody = async {
        let req = SendMessageRequest(QueueUrl = queueUri, MessageBody = msgBody)
        let! ct = Async.CancellationToken
        do! account.SQSClient.SendMessageAsync(req, ct) 
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    let private dequeueInternal (account : AwsSQSAccount) queueUri (timeout : int option) = async {
        let timeout = defaultArg timeout SqsConstants.maxWaitTime
        let req = ReceiveMessageRequest(QueueUrl = queueUri)
        req.MaxNumberOfMessages <- 1
        
        // always make use of long polling for efficiency
        // but never wait for more than max allowed (20 seconds)
        req.WaitTimeSeconds <- min SqsConstants.maxWaitTime timeout

        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.ReceiveMessageAsync(req, ct)
                   |> Async.AwaitTaskCorrect
        if res.Messages.Count = 1
        then return Some <| res.Messages.[0]
        else return None
    }

    let dequeue (account : AwsSQSAccount) queueUri (timeout : int option) = async {
        let! msg = dequeueInternal account queueUri timeout

        match msg with
        | Some msg -> return Some msg.Body
        | _ -> return None
    }

    let dequeueWithAttributes (account : AwsSQSAccount) queueUri (timeout : int option) = async {
        let! msg = dequeueInternal account queueUri timeout

        match msg with
        | Some msg -> return Some (msg.ReceiptHandle, msg.Body, msg.Attributes)
        | _ -> return None
    }

    let getCount (account : AwsSQSAccount) queueUri = async {
        let req = GetQueueAttributesRequest(QueueUrl = queueUri)
        let attrName = QueueAttributeName.ApproximateNumberOfMessages.Value
        req.AttributeNames.Add(attrName)

        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.GetQueueAttributesAsync(req, ct)
                   |> Async.AwaitTaskCorrect
        return int64 res.ApproximateNumberOfMessages
    }

    let deleteQueue (account : AwsSQSAccount) queueUri = async {
        let req = DeleteQueueRequest(QueueUrl = queueUri)
        let! ct = Async.CancellationToken
        do! account.SQSClient.DeleteQueueAsync(req, ct)
            |> Async.AwaitTaskCorrect
            |> Async.Ignore
    }

    let tryGetQueueUri (account : AwsSQSAccount) queueName = async {
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

    let doesQueueExist (account : AwsSQSAccount) queueName = async {
        let! queueUri = tryGetQueueUri account queueName
        match queueUri with
        | Some _ -> return true
        | _      -> return false
    }

    let createQueue (account : AwsSQSAccount) queueName = async {
        let req  = CreateQueueRequest(QueueName = queueName)
        let! ct  = Async.CancellationToken
        let! res = account.SQSClient.CreateQueueAsync(req, ct)
                   |> Async.AwaitTaskCorrect
        return res.QueueUrl
    }

    let createIfNotExist (account : AwsSQSAccount) queueName = async {
        let! queueUri = tryGetQueueUri account queueName
        match queueUri with
        | Some queueUri -> return queueUri
        | _             -> return! createQueue account queueName
    }