namespace MBrace.AWS

open System
open System.Runtime.Serialization

open Amazon
open Amazon.Runtime

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

/// Serializable AWS credentials record
[<NoEquality; NoComparison>]
type AWSCredentials = 
    {
        /// AWS account Access Key
        AccessKey :string
        /// AWS account Secret Key
        SecretKey : string
    }
with
    member internal __.Credentials = new BasicAWSCredentials(__.AccessKey, __.SecretKey) :> Amazon.Runtime.AWSCredentials

    static member FromCredentialStore(?profileName : string) =
        let profileName = defaultArg profileName "default"
        let creds = Amazon.Util.ProfileManager.GetAWSCredentials(profileName).GetCredentials()
        { AccessKey = creds.AccessKey ; SecretKey = creds.SecretKey }