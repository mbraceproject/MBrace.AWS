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
type SQSCloudQueue<'T> internal (queueUri, account : AWSAccount) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueueUri")>]
    let queueUri = queueUri

    let client() = account.SQSClient
    
    interface CloudQueue<'T> with
        member __.Id = queueUri

        member __.EnqueueAsync(message : 'T) = 
            client().Enqueue (queueUri, toBase64 message)
        
        member __.EnqueueBatchAsync(messages) = async {
            let msgBodies = messages |> Seq.map (fun m -> toBase64 m, None)
            do! client().EnqueueBatch(queueUri, msgBodies)
        }
        
        member __.DequeueAsync(timeout) = async {
            let client = client()
            match timeout with
            | Some _ ->
                let! msg = client.TryDequeue(queueUri, ?timeoutMilliseconds = timeout)
                match msg with
                | Some m -> 
                    do! m.Complete()
                    return fromBase64<'T> m.Message.Body
                | None -> return! Async.Raise(TimeoutException())
            | _ -> 
                let rec aux _ = async {
                    let! msg = client.TryDequeue(queueUri)
                    match msg with
                    | Some m ->
                        do! m.Complete()
                        return fromBase64<'T> m.Message.Body

                    | None -> return! aux()
                }
                return! aux ()
        }

        member __.DequeueBatchAsync(maxItems) = async {
            let client = client()
            let! messages = client.DequeueBatch(queueUri, maxReceiveCount = maxItems)
            do! client.DeleteBatch(messages)
            return messages
                   |> Seq.map (fun msg -> fromBase64<'T> msg.Message.Body) 
                   |> Seq.toArray
        }
        
        member __.TryDequeueAsync() = async {
            let! msg = client().TryDequeue(queueUri)
            match msg with
            | Some msg -> 
                do! msg.Complete()
                return Some <| fromBase64<'T> msg.Message.Body
            | _ -> return None
        }

        member x.GetCountAsync() = async {
            let! c = client().GetMessageCount(queueUri)
            return int64 c
        }

        member x.Dispose() = client().DeleteQueueUri(queueUri)

[<Sealed; DataContract>]
type SQSCloudQueueProvider private (account : AWSAccount, queuePrefix : string) =
    [<DataMember(Name = "Account")>]
    let account = account

    [<DataMember(Name = "QueuePrefix")>]
    let queuePrefix = queuePrefix

    static member Create(account : AWSAccount, ?queuePrefix : string) = 
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