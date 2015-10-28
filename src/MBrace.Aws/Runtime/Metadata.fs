namespace MBrace.Aws.Runtime

open System

open MBrace.Aws

type Metadata =
    {
        Version   : Version
        ClusterId : ClusterId
    }

    static member Create(id : ClusterId, ?version : Version) = 
        { 
            Version   = defaultArg version ProcessConfiguration.Version
            ClusterId = id 
        }

    static member ToJson (metadata : Metadata) =
        ProcessConfiguration.JsonSerializer.PickleToString metadata

    static member FromJson (metadata : string) =
        try ProcessConfiguration.JsonSerializer.UnPickleOfString<Metadata> metadata
        with e -> raise <| FormatException(sprintf "Failed to deserialize metadata %s" metadata, e)