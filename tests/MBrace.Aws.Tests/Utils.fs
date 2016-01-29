namespace MBrace.AWS.Tests

open System
open System.IO
open NUnit.Framework

open Amazon
open Amazon.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Tests

open MBrace.Runtime
open MBrace.Runtime.Components
open MBrace.ThreadPool

open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Store

[<AutoOpen>]
module Utils =

    let init() = ProcessConfiguration.InitAsClient()

    let getEnvironmentVariable (envName:string) =
        let aux found target =
            if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
            else found

        Array.fold aux null [|EnvironmentVariableTarget.Process; EnvironmentVariableTarget.User; EnvironmentVariableTarget.Machine|]
        
    let getEnvironmentVariableOrDefault envName defaultValue = 
        match getEnvironmentVariable envName with
        | null | "" -> defaultValue
        | ev -> ev

    let getTestRegion () = 
        match getEnvironmentVariable "MBrace.AWS.Tests.Region" with
        | null | "" -> RegionEndpoint.EUCentral1
        | region -> RegionEndpoint.GetBySystemName region

    let getAWSProfile () = getEnvironmentVariableOrDefault "MBrace.AWS.Test.ProfileName" "default"
    let tryGetAWSCredentials () = 
        match getEnvironmentVariable "MBrace.AWS.Test.Credentials" with
        | null | "" -> None
        | creds -> 
            let toks = creds.Split(',')
            let creds = new BasicAWSCredentials(toks.[0], toks.[1]) :> AWSCredentials
            Some creds

    let getAWSTestAccount () =
        let region = getTestRegion()
        let profile = getAWSProfile()
        AWSAccount.Create(profile, region, ?credentials = tryGetAWSCredentials())