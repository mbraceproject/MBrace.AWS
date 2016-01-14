namespace MBrace.Aws.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization

open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel

open MBrace.Runtime.Utils

[<AutoSerializable(false); NoEquality; NoComparison>]
type private AwsAccountData = 
    {
        ProfileName     : string
        Region          : RegionEndpoint
        Credentials     : ImmutableCredentials
        S3Client        : AmazonS3Client
        DynamoDBClient  : AmazonDynamoDBClient
        SQSClient       : AmazonSQSClient
    }

/// Defines a serializable AWS account descriptor which does not leak credentials
[<Sealed; DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type AwsAccount private (accountData : AwsAccountData) =
    static let localRegistry = new ConcurrentDictionary<string * string, AwsAccountData>()
    static let mkKey (profileName : string) (region : RegionEndpoint) = profileName, region.SystemName

    static let initAccountData (profileName : string) (region : RegionEndpoint) (credentials : AWSCredentials) =
        {
            ProfileName = profileName
            Region = region
            Credentials = credentials.GetCredentials()
            S3Client = new AmazonS3Client(credentials, region)
            DynamoDBClient = new AmazonDynamoDBClient(credentials, region)
            SQSClient = new AmazonSQSClient(credentials, region)
        }

    static let recoverAccountData (profileName : string) (region : RegionEndpoint) =
        let k = mkKey profileName region
        let ok, found = localRegistry.TryGetValue k
        if ok then found 
        else
            let initFromProfileManager _ =
                let credentials = Amazon.Util.ProfileManager.GetAWSCredentials profileName
                initAccountData profileName region credentials

            localRegistry.GetOrAdd(k, initFromProfileManager)

    [<DataMember(Name = "ProfileName")>]
    let profileName = accountData.ProfileName
    [<DataMember(Name = "RegionName")>]
    let regionName = accountData.Region.SystemName
    [<IgnoreDataMember>]
    let mutable localData = Some accountData

    let getAccountData() =
        match localData with
        | Some ld -> ld
        | None -> 
            let ld = recoverAccountData profileName (RegionEndpoint.GetBySystemName regionName)
            localData <- Some ld
            ld

    /// AWS account profile identifier
    member __.ProfileName = profileName
    member private __.RegionName = regionName
    /// AWS account access Key
    member __.AccessKey = getAccountData().Credentials.AccessKey
    /// Credentials for AWS account
    member __.Credentials = let c = getAccountData().Credentials in new BasicAWSCredentials(c.AccessKey, c.SecretKey) :> AWSCredentials
    /// Region endpoint identifier for account
    member __.Region = getAccountData().Region
    /// Amazon S3 Client instance for account
    member __.S3Client = getAccountData().S3Client :> IAmazonS3
    /// Amazon SQS Client instance for account
    member __.SQSClient = getAccountData().SQSClient :> IAmazonSQS
    /// Amazon DynamoDB Client instance for account
    member __.DynamoDBClient = getAccountData().DynamoDBClient :> IAmazonDynamoDB

    interface IComparable with
        member __.CompareTo(other:obj) =
            match other with
            | :? AwsAccount as aa -> compare2 profileName regionName aa.ProfileName aa.RegionName
            | _ -> invalidArg "other" "invalid comparand."

    override __.Equals(other:obj) =
        match other with
        | :? AwsAccount as aa -> profileName = aa.ProfileName && regionName = aa.RegionName
        | _ -> false

    override __.GetHashCode() = hash2 profileName regionName

    member private __.StructuredFormatDisplay = sprintf "AWS Profile %s, Region %s" profileName regionName
    override __.ToString() = __.StructuredFormatDisplay

    /// <summary>
    ///     Creates a new AWS credentials with provided profile name and region endpoint.
    ///     If credentials object is not supplied, it will be recovered from the local profile manager.
    /// </summary>
    /// <param name="profileName">Profile identifier.</param>
    /// <param name="region">Region endpoint.</param>
    /// <param name="credentials">AWS credentials for profile. Defaults to credentials recovered from local profile manager.</param>
    static member Create(profileName : string, region : RegionEndpoint, ?credentials : AWSCredentials) =
        let initAccountData _ =
            let credentials =
                match credentials with
                | Some c -> c
                | None -> Amazon.Util.ProfileManager.GetAWSCredentials profileName

            initAccountData profileName region credentials

        let accountData = localRegistry.GetOrAdd(mkKey profileName region, initAccountData)
        new AwsAccount(accountData)