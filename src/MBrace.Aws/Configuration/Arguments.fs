module MBrace.AWS.Runtime.Arguments

open System
open System.IO

open Argu

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.Runtime

/// Argu configuration schema
type private AWSArguments =
    // General-purpose arguments
    | [<AltCommandLine("-w")>] Worker_Id of string
    | [<AltCommandLine("-m")>] Max_Work_Items of int
    | Heartbeat_Interval of float
    | Heartbeat_Threshold of float
    | [<AltCommandLine("-q")>] Quiet
    | [<AltCommandLine("-L")>] Log_Level of int
    | [<AltCommandLine("-l")>] Log_File of string
    | Working_Directory of string
    // Global parameters
    | Resource_Prefix of string
    | Credentials of accessKey:string * secretKey:string
    | Profile of profileName:string
    | [<Mandatory>] Region of region:string
    // S3 parameters
    | S3_Credentials of accessKey:string * secretKey:string
    | S3_Profile of profileName:string
    | S3_Region of region:string
    // SQS parameters
    | SQS_Credentials of accessKey:string * secretKey:string
    | SQS_Profile of profileName:string
    | SQS_Region of region:string
    // DynamoDB parameters
    | DynamoDB_Credentials of accessKey:string * secretKey:string
    | DynamoDB_Profile of profileName:string
    | DynamoDB_Region of region:string
    // Cluster configuration parameters
    | Optimize_Closure_Serialization of bool
    // SQS Params
    | Runtime_Queue of string
    | Runtime_Topic of string
    // S3 Params
    | Runtime_Bucket of string
    | User_Data_Bucket of string
    // DynamoDB params
    | Runtime_Table of string
    | Runtime_Logs_Table of string
    | User_Data_Table of string

    interface IArgParserTemplate with
        member arg.Usage =
            match arg with
            | Quiet -> "Suppress logging to stdout by the worker."
            | Resource_Prefix _ -> "Specifies a unique cluster identifier which acts as an AWS resource prefix."
            | Log_Level _ -> "Log level for worker system logs. Critical = 1, Error = 2, Warning = 3, Info = 4, Debug = 5. Defaults to info."
            | Log_File _ -> "Specify a log file to write worker system logs."
            | Max_Work_Items _ -> "Specify maximum number of concurrent work items."
            | Heartbeat_Interval _ -> "Specify the heartbeat interval for the worker in seconds. Defaults to 1 second."
            | Heartbeat_Threshold _ -> "Specify the heartbeat interval for the worker in seconds. Defaults to 300 seconds."
            | Working_Directory _ -> "Specify the working directory for the worker."
            | Worker_Id _ -> "Specify worker name identifier."
            | Credentials _ -> "Specify the default AWS credentials for the worker."
            | Profile _ -> "Specify a default AWS profile name for the worker."
            | Region _ -> "Specify the default AWS region for the worker."
            | S3_Credentials _ -> "Specify an alternate set of AWS credentials for use with S3."
            | S3_Profile _ -> "Specify an alternate AWS profile name for use with S3."
            | S3_Region _ -> "Specify an alternate AWS region for use with S3."
            | SQS_Credentials _ -> "Specify an alternate set of AWS credentials for use with SQS."
            | SQS_Profile _ -> "Specify an alternate AWS profile name for use with SQS."
            | SQS_Region _ -> "Specify an alternate AWS region for use with SQS."
            | DynamoDB_Credentials _ -> "Specify an alternate set of AWS credentials for use with DynamoDB."
            | DynamoDB_Profile _ -> "Specify an alternate AWS profile name for use with DynamoDB."
            | DynamoDB_Region _ -> "Specify an alternate AWS region for use with DynamoDB."
            | Optimize_Closure_Serialization _ -> "Specifies whether cluster should implement closure serialization optimizations. Defaults to true."
            | Runtime_Queue _ -> "Specifies the work item queue name in SQS."
            | Runtime_Topic _ -> "Specifies the work item topic name in SQS."
            | Runtime_Bucket _ -> "Specifies the S3 bucket name used for persisting MBrace cluster data."
            | User_Data_Bucket _ -> "Specifies the S3 bucket name used for persisting MBrace user data."
            | Runtime_Table _ -> "Specifies the table name used for writing MBrace cluster entries."
            | Runtime_Logs_Table _ -> "Specifies the table name used for writing MBrace cluster system log entries."
            | User_Data_Table _ -> "Specifies the table name used for writing user logs."


let private argParser = ArgumentParser.Create<AWSArguments>()

/// Configuration object encoding command line parameters for an MBrace.AWS process
type ArgumentConfiguration = 
    {
        Quiet : bool
        Configuration : Configuration option
        MaxWorkItems : int option
        WorkerId : string option
        LogLevel : LogLevel option
        LogFile : string option
        HeartbeatInterval : TimeSpan option
        HeartbeatThreshold : TimeSpan option
        WorkingDirectory : string option
    }

    /// Creates a configuration object using supplied parameters.
    static member Create(?config : Configuration, ?quiet : bool, ?workingDirectory : string, ?maxWorkItems : int, ?workerId : string, ?logLevel : LogLevel, 
                            ?logfile : string, ?heartbeatInterval : TimeSpan, ?heartbeatThreshold : TimeSpan) =
        maxWorkItems |> Option.iter (fun w -> if w < 0 then invalidArg "maxWorkItems" "must be positive." elif w > 1024 then invalidArg "maxWorkItems" "exceeds 1024 limit.")
        heartbeatInterval |> Option.iter (fun i -> if i < TimeSpan.FromSeconds 1. then invalidArg "heartbeatInterval" "must be at least one second.")
        heartbeatThreshold |> Option.iter (fun i -> if i < TimeSpan.FromSeconds 1. then invalidArg "heartbeatThreshold" "must be at least one second.")
        workerId |> Option.iter Validate.hostname
        let workingDirectory = workingDirectory |> Option.map Path.GetFullPath
        let quiet = defaultArg quiet false
        { Configuration = config ; MaxWorkItems = maxWorkItems ; WorkerId = workerId ; LogFile = logfile ;
            LogLevel = logLevel ; HeartbeatInterval = heartbeatInterval ; HeartbeatThreshold = heartbeatThreshold ;
            WorkingDirectory = workingDirectory ; Quiet = quiet }

    /// Converts a configuration object to a command line string.
    static member ToCommandLineArguments(cfg : ArgumentConfiguration) =
        let args = [

            match cfg.MaxWorkItems with Some w -> yield Max_Work_Items w | None -> ()
            match cfg.WorkerId with Some n -> yield Worker_Id n | None -> ()
            match cfg.LogLevel with Some l -> yield Log_Level (int l) | None -> ()
            match cfg.HeartbeatInterval with Some h -> yield Heartbeat_Interval h.TotalSeconds | None -> ()
            match cfg.HeartbeatThreshold with Some h -> yield Heartbeat_Threshold h.TotalSeconds | None -> ()
            match cfg.LogFile with Some l -> yield Log_File l | None -> ()
            match cfg.WorkingDirectory with Some w -> yield Working_Directory w | None -> ()

            match cfg.Configuration with
            | None -> ()
            | Some config ->
                let inline ec (creds : AWSCredentials) = creds.AccessKey, creds.SecretKey
                let inline er (r : AWSRegion) = r.SystemName
                yield Credentials (ec config.DefaultCredentials)
                yield Region (er config.DefaultRegion)
                yield Resource_Prefix config.ResourcePrefix

                yield S3_Credentials (ec config.S3Credentials)
                yield S3_Region (er config.S3Region)

                yield DynamoDB_Credentials (ec config.DynamoDBCredentials)
                yield DynamoDB_Region (er config.DynamoDBRegion)

                yield SQS_Credentials (ec config.SQSCredentials)
                yield SQS_Region (er config.SQSRegion)

                yield Optimize_Closure_Serialization config.OptimizeClosureSerialization

                yield Runtime_Queue config.WorkItemQueue
                yield Runtime_Topic config.WorkItemTopic

                yield Runtime_Bucket config.RuntimeBucket
                yield User_Data_Bucket config.UserDataBucket

                yield Runtime_Table config.RuntimeTable
                yield Runtime_Logs_Table config.RuntimeLogsTable
                yield User_Data_Table config.UserDataTable
        ]

        argParser.PrintCommandLineFlat args

    /// Parses command line arguments to a configuration object using Argu.
    static member FromCommandLineArguments(args : string []) =
        let parseResult = argParser.Parse(args, errorHandler = new ProcessExiter())

        let maxWorkItems = parseResult.TryPostProcessResult(<@ Max_Work_Items @>, fun i -> if i < 0 then failwith "must be positive." elif i > 1024 then failwith "exceeds 1024 limit." else i)
        let quiet = parseResult.Contains <@ Quiet @>
        let logLevel = parseResult.TryPostProcessResult(<@ Log_Level @>, enum<LogLevel>)
        let logFile = parseResult.TryPostProcessResult(<@ Log_File @>, fun f -> ignore <| Path.GetFullPath f ; f) // use GetFullPath to validate chars
        let workerName = parseResult.TryPostProcessResult(<@ Worker_Id @>, fun name -> Validate.hostname name; name)
        let heartbeatInterval = parseResult.TryPostProcessResult(<@ Heartbeat_Interval @>, fun i -> let t = TimeSpan.FromSeconds i in if t < TimeSpan.FromSeconds 1. then failwith "must be positive" else t)
        let heartbeatThreshold = parseResult.TryPostProcessResult(<@ Heartbeat_Threshold @>, fun i -> let t = TimeSpan.FromSeconds i in if t < TimeSpan.FromSeconds 1. then failwith "must be positive" else t)
        let workingDirectory = parseResult.TryPostProcessResult(<@ Working_Directory @>, Path.GetFullPath)

        let clusterId = parseResult.TryPostProcessResult(<@ Resource_Prefix @>, fun id -> Validate.hostname id; id)
        let defaultRegion = parseResult.PostProcessResult(<@ Region @>, AWSRegion.Parse)
        let credentials =
            match parseResult.TryGetResult <@ Credentials @> with
            | Some(ak, sk) -> { AccessKey = ak ; SecretKey = sk }
            | None ->
                match parseResult.TryPostProcessResult(<@ Profile @>, fun pf -> AWSCredentials.FromCredentialStore pf) with
                | Some creds -> creds
                | None -> AWSCredentials.FromCredentialStore |> parseResult.Catch

        let config = new Configuration(defaultRegion, credentials, ?resourcePrefix = clusterId)

        let iterRegion c f = parseResult.IterResult(c, f << AWSRegion.Parse)
        iterRegion <@ DynamoDB_Region @> (fun r -> config.DynamoDBRegion <- r)
        iterRegion <@ S3_Region @> (fun r -> config.S3Region <- r)
        iterRegion <@ SQS_Region @> (fun r -> config.SQSRegion <- r)

        let iterCreds c c' f = 
            parseResult.IterResult(c, fun (ak,sk) -> f { AccessKey = ak ; SecretKey = sk })
            parseResult.IterResult(c', fun p -> f (AWSCredentials.FromCredentialStore p))

        iterCreds <@ DynamoDB_Credentials @> <@ DynamoDB_Profile @> (fun c -> config.DynamoDBCredentials <- c)
        iterCreds <@ S3_Credentials @> <@ S3_Profile @> (fun c -> config.S3Credentials <- c)
        iterCreds <@ SQS_Credentials @> <@ SQS_Profile @> (fun c -> config.SQSCredentials <- c)

        parseResult.IterResult(<@ Optimize_Closure_Serialization @>, fun o -> config.OptimizeClosureSerialization <- o)

        parseResult.IterResult(<@ Runtime_Queue @>, fun q -> config.WorkItemQueue <- q)
        parseResult.IterResult(<@ Runtime_Topic @>, fun t -> config.WorkItemTopic <- t)

        parseResult.IterResult(<@ Runtime_Bucket @>, fun c -> config.RuntimeBucket <- c)
        parseResult.IterResult(<@ User_Data_Bucket @>, fun c -> config.UserDataBucket <- c)

        parseResult.IterResult(<@ Runtime_Table @>, fun c -> config.RuntimeTable <- c)
        parseResult.IterResult(<@ Runtime_Logs_Table @>, fun c -> config.RuntimeLogsTable <- c)
        parseResult.IterResult(<@ User_Data_Table @>, fun c -> config.UserDataTable <- c)

        {
            Configuration = Some config
            MaxWorkItems = maxWorkItems
            WorkerId = workerName
            WorkingDirectory = workingDirectory
            Quiet = quiet
            LogLevel = logLevel
            LogFile = logFile
            HeartbeatInterval = heartbeatInterval
            HeartbeatThreshold = heartbeatThreshold
        }