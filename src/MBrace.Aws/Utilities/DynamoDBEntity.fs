namespace MBrace.Aws.Runtime.Utilities

open System

type DynamoDBEntity =
    {
        HashKey : string
        Blob    : byte[]
        LastModified : DateTime
    }