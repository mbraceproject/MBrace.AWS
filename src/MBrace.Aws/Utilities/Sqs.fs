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

    let dequeue (account : AwsSQSAccount) queueUri (timeout : int option) = async {
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
        then return Some <| res.Messages.[0].Body
        else return None
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