namespace MBrace.Aws.Runtime.Utilities

open Amazon.DynamoDBv2.DocumentModel

open MBrace.Core.Internals
open MBrace.Aws.Runtime

[<AllowNullLiteral>]
type IDynamoDBDocument =
    abstract member ToDynamoDBDocument : unit -> Document

[<RequireQualifiedAccess>]
module internal Table =
    // NOTE: implement a specific put rather than reinvent the object persistence layer as that's a lot
    // of work and at this point not enough payoff
    let inline update (account : AwsDynamoDBAccount) tableName (entity : IDynamoDBDocument) =
        async { 
            let table = Table.LoadTable(account.DynamoDBClient, tableName)
            let doc   = entity.ToDynamoDBDocument()
            let! ct   = Async.CancellationToken

            do! table.UpdateItemAsync(doc, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Ignore
        }