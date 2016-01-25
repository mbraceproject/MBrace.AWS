namespace MBrace.AWS.Store

open System
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Amazon.SQS
open Amazon.SQS.Model

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.PrettyPrinters

open MBrace.AWS.Runtime
open MBrace.AWS.Runtime.Utilities

[<AutoOpen>]
module private SQSQueueImpl =

    let mkRandomQueueName prefix = sprintf "%s-%s" prefix <| Guid.NewGuid().ToString("N")
    let mkRandomQueueNameRegex prefix = new Regex(sprintf "%s-[0-9a-z]{32}" prefix, RegexOptions.Compiled)

    let queueNameRegex = new Regex("https://sqs\.([^\./]+)\.amazonaws\.com/([^/]+)/([^/]+)", RegexOptions.Compiled)
    let getQueueName (id : string) =
        let m = queueNameRegex.Match id
        if m.Success then m.Groups.[3].Value
        else id


/// CloudQueue implementation on top of Amazon SQS
[<Sealed; DataContract>]
type SQSCloudQueue<'T> internal (queueUri, account : AwsAccount) =
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

[<Sealed; DataContract>]
type SQSCloudQueueProvider private (account : AwsAccount, queuePrefix : string) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueuePrefix")>]
    let queuePrefix = queuePrefix

    static member Create(account : AwsAccount, ?queuePrefix : string) = 
        let queuePrefix = defaultArg queuePrefix "mbrace"
        new SQSCloudQueueProvider(account, queuePrefix)


    /// <summary>
    ///     Clears all randomly named SQS queues that match the given prefix.
    /// </summary>
    /// <param name="prefix">Prefix to clear. Defaults to the queue prefix of the current store instance.</param>
    member this.ClearQueuesAsync(?prefix : string) = async {
        let queuePrefix = defaultArg prefix queuePrefix
        let! ct = Async.CancellationToken
        let! queues = account.SQSClient.ListQueuesAsync(queuePrefix, ct) |> Async.AwaitTaskCorrect
        do! queues.QueueUrls
            |> Seq.map (fun url -> account.SQSClient.DeleteQueueAsync(url, ct) |> Async.AwaitTaskCorrect)
            |> Async.Parallel
            |> Async.Ignore
    }

    interface ICloudQueueProvider with
        member x.Name = "SQS CloudQueue Provider"
        member x.Id = account.ToString()
        member x.CreateQueue<'T>(queueId : string) = async {
            let queueId = getQueueName queueId
            let! ct = Async.CancellationToken
            let! response = account.SQSClient.CreateQueueAsync(queueId, ct) |> Async.AwaitTaskCorrect
            return new SQSCloudQueue<'T>(response.QueueUrl, account) :> CloudQueue<'T>
        }

        member x.GetQueueById(queueId : string) = async {
            let queueId = getQueueName queueId
            let! ct = Async.CancellationToken
            let! response = account.SQSClient.GetQueueUrlAsync(queueId, ct) |> Async.AwaitTaskCorrect
            return new SQSCloudQueue<'T>(response.QueueUrl, account) :> CloudQueue<'T>
        }
        member x.GetRandomQueueName() = mkRandomQueueName queuePrefix