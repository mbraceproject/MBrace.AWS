namespace MBrace.Aws.Runtime

open System.Text.RegularExpressions

[<RequireQualifiedAccess>]
module Validate =
    let private tableNameRegex = Regex("^[a-zA-Z0-9\-_\.]*$", RegexOptions.Compiled)

    let inline private validate (r : Regex) (input : string) = r.IsMatch input

    // DynamoDB Name limitations, see:
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
    let tableName (tableName : string) =
        let exn () = 
            sprintf "Invalid DynamoDB table name, see %s" 
                    "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html"
            |> invalidArg "tableName" 
        if tableName.Length < 3 || tableName.Length > 255 then exn()
        if not <| validate tableNameRegex tableName then exn()