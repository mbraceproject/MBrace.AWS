namespace MBrace.AWS

open System
open System.IO
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Amazon
open Amazon.Util
open Amazon.Runtime

open MBrace.Runtime.Utils
open MBrace.AWS.Runtime

/// Serializable wrapper for AWS RegionEndpoint
[<Sealed; DataContract>]
type AWSRegion (region : RegionEndpoint) =
    static let mk r = new AWSRegion(r)
    [<DataMember(Name = "SystemName")>]
    let name = region.SystemName
    member __.SystemName = name
    member internal __.RegionEndpoint = RegionEndpoint.GetBySystemName name

    override __.GetHashCode() = hash name
    override __.Equals y =
        match y with
        | :? AWSRegion as r -> name = r.SystemName
        | _ -> false

    interface IComparable with
        member __.CompareTo y =
            match y with
            | :? AWSRegion as r -> compare name r.SystemName
            | _ -> invalidArg "y" "invalid comparand"

    override __.ToString() = name

    static member Parse(name : string) =
        let ep = RegionEndpoint.GetBySystemName name
        new AWSRegion(ep)

    /// The Asia Pacific (Tokyo) endpoint.
    static member APNortheast1 = mk RegionEndpoint.APNortheast1
    /// The Asia Pacific (Seoul) endpoint.
    static member APNortheast2 = mk RegionEndpoint.APNortheast2
    /// The Asia Pacific (Singapore) endpoint.
    static member APSoutheast1 = mk RegionEndpoint.APSoutheast1
    /// The Asia Pacific (Sydney) endpoint.
    static member APSoutheast2 = mk RegionEndpoint.APSoutheast2
    /// The China (Beijing) endpoint.
    static member CNNorth1 = mk RegionEndpoint.CNNorth1
    /// The EU Central (Frankfurt) endpoint.
    static member EUCentral1 = mk RegionEndpoint.EUCentral1
    /// The South America (Sao Paulo) endpoint.
    static member SAEast1 = mk RegionEndpoint.SAEast1
    /// The US East (Virginia) endpoint.
    static member USEast1 = mk RegionEndpoint.USEast1
    /// The EU West (Ireland) endpoint.
    static member EUWest1 = mk RegionEndpoint.EUWest1
    /// The US GovCloud West (Oregon) endpoint.
    static member USGovCloudWest1 = mk RegionEndpoint.USGovCloudWest1
    /// The US West (N. California) endpoint.
    static member USWest1 = mk RegionEndpoint.USWest1
    /// The US West (Oregon) endpoint.
    static member USWest2 = mk RegionEndpoint.USWest2

/// Serializable AWS credentials container class
[<Sealed; DataContract>]
type MBraceAWSCredentials (accessKey : string, secretKey : string) =
    inherit Amazon.Runtime.AWSCredentials()

    let [<DataMember(Name = "AccessKey")>] accessKey = accessKey
    let [<DataMember(Name = "SecretKey")>] secretKey = secretKey

    member __.AccessKey = accessKey
    member __.SecretKey = secretKey

    override __.GetCredentials() = new ImmutableCredentials(accessKey, secretKey, null)
    override __.GetCredentialsAsync() = System.Threading.Tasks.Task.Run __.GetCredentials

    new (immutableCredentials : ImmutableCredentials) =
        new MBraceAWSCredentials(immutableCredentials.AccessKey, immutableCredentials.SecretKey)

    new (credentials : AWSCredentials) =
        new MBraceAWSCredentials(credentials.GetCredentials())

    /// <summary>
    ///     Recover a set of credentials using the local credentials store.
    /// </summary>
    /// <param name="profileName">Credential store profile name. Defaults to 'default' profile.</param>
    static member FromCredentialsStore([<O;D(null)>]?profileName : string) =
        let profileName = defaultArg profileName "default"
        let ok, creds = ProfileManager.TryGetAWSCredentials(profileName)
        if ok then new MBraceAWSCredentials(creds)
        else
            let credsFile = Path.Combine(getHomePath(), ".aws", "credentials")
            if not <| File.Exists credsFile then
                sprintf "Could not locate stored credentials profile '%s'." profileName |> invalidOp

            let text = File.ReadAllText credsFile

            let matchingProfile =
                Regex.Matches(text, "\[(\S+)\]\s+aws_access_key_id\s*=\s*(\S+)\s+aws_secret_access_key\s*=\s*(\S+)")
                |> Seq.cast<Match>
                |> Seq.map (fun m -> m.Groups.[1].Value, m.Groups.[2].Value, m.Groups.[3].Value)
                |> Seq.tryFind (fun (pf,_,_) -> pf = profileName)

            match matchingProfile with
            | None -> sprintf "Could not locate stored credentials profile '%s'." profileName |> invalidOp
            | Some (_,aK,sK) -> new MBraceAWSCredentials(aK, sK)

    /// <summary>
    ///     Recovers a credentials instance from the local environment
    ///     using the the 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' variables.
    /// </summary>
    static member FromEnvironmentVariables() =
        let accessKeyName = "AWS_ACCESS_KEY_ID"
        let secretKeyName = "AWS_SECRET_ACCESS_KEY"

        let getEnv (envName:string) =
            let aux found target =
                if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
                else found

            Array.fold aux null [|
                EnvironmentVariableTarget.Process; 
                EnvironmentVariableTarget.User; 
                EnvironmentVariableTarget.Machine |]

        match getEnv accessKeyName, getEnv secretKeyName with
        | null, null -> sprintf "Undefined environment variables '%s' and '%s'" accessKeyName secretKeyName |> invalidOp
        | null, _ -> sprintf "Undefined environment variable '%s'" accessKeyName |> invalidOp
        | _, null -> sprintf "Undefined environment variable '%s'" secretKeyName |> invalidOp
        | aK, sK  -> new MBraceAWSCredentials(aK, sK)


/// MBrace.AWS Configuration Builder. Used to specify MBrace.AWS cluster storage configuration.
[<AutoSerializable(true); Sealed; NoEquality; NoComparison>]
type Configuration private (region : AWSRegion, credentials : AWSCredentials, ?resourcePrefix : string) =
    let credentials = MBraceAWSCredentials(credentials)
    let resourcePrefix = 
        match resourcePrefix with
        | Some rp -> Validate.hostname rp ; rp
        | None -> 
            let version = ProcessConfiguration.Version
            sprintf "v%dm%d" version.Major version.Minor
        
    let mkName sep name = sprintf "%s%s%s" name sep resourcePrefix

    // Default SQS queue names
    let mutable workItemQueue        = mkName "-" "MBraceWorkItemQueue"
    let mutable workItemTopic        = mkName "-" "MBraceWorkItemTopic"

    // Default S3 bucket names
    let mutable runtimeBucket    = mkName "." "mbraceruntimedata"
    let mutable userDataBucket   = mkName "." "mbraceuserdata"

    // Default DynamoDB table names
    let mutable userDataTable       = mkName "." "MBraceUserData"
    let mutable runtimeTable        = mkName "." "MBraceRuntimeData"
    let mutable runtimeLogsTable    = mkName "." "MBraceRuntimeLogs"

    member __.ResourcePrefix = resourcePrefix

    member __.DefaultCredentials = credentials
    member __.DefaultRegion = region

    /// AWS S3 Account credentials
    member val S3Credentials = credentials with get, set
    member val S3Region = region with get, set

    /// AWS DynamoDB Account credentials
    member val DynamoDBCredentials = credentials with get, set
    member val DynamoDBRegion = region with get, set

    /// AWS SQS Account credentials
    member val SQSCredentials = credentials with get, set
    member val SQSRegion = region with get, set

    /// Specifies wether the cluster should optimize closure serialization. Defaults to true.
    member val OptimizeClosureSerialization = true with get, set

    /// SQS work item queue used by the runtime.
    member __.WorkItemQueue
        with get () = workItemQueue
        and set rq = workItemQueue <- rq

    /// SQS work item topic used by the runtime.
    member __.WorkItemTopic
        with get () = workItemTopic
        and set rt = workItemTopic <- rt

    /// S3 bucket used by the runtime.
    member __.RuntimeBucket
        with get () = runtimeBucket
        and set rc = Validate.bucketName rc ; runtimeBucket <- rc

    /// S3 bucket used for user data.
    member __.UserDataBucket
        with get () = userDataBucket
        and set udb = Validate.bucketName udb ; userDataBucket <- udb

    /// DynamoDB table used by the runtime.
    member __.RuntimeTable
        with get () = runtimeTable
        and set rt = Validate.tableName rt; runtimeTable <- rt

    /// DynamoDB table used by the runtime for storing logs.
    member __.RuntimeLogsTable
        with get () = runtimeLogsTable
        and set rlt = Validate.tableName rlt ; runtimeLogsTable <- rlt

    /// DynamoDB table used for user data.
    member __.UserDataTable
        with get () = userDataTable
        and set udt = Validate.tableName udt ; userDataTable <- udt

    /// <summary>
    ///     Defines an initial MBrace.AWS configuration object using specified initial paramters.
    ///     The resulting configuration object uniquely identifies an MBrace.AWS cluster entity.
    /// </summary>
    /// <param name="region">AWS region identifier.</param>
    /// <param name="credentials">AWS credentials used by the cluster.</param>
    /// <param name="resourcePrefix">Resource prefix used by the cluster.</param>
    static member Define(region : AWSRegion, credentials : AWSCredentials, [<O;D(null)>]?resourcePrefix : string) =
        new Configuration(region, credentials, ?resourcePrefix = resourcePrefix)