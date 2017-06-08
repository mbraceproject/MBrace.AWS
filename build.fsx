// --------------------------------------------------------------------------------------
// FAKE build script
// --------------------------------------------------------------------------------------

#r @"packages/build/FAKE/tools/FakeLib.dll"
open Fake
open Fake.AppVeyor
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open Fake.UserInputHelper
open System
open System.IO

// --------------------------------------------------------------------------------------
// START TODO: Provide project-specific details below
// --------------------------------------------------------------------------------------

// Information about the project are used
//  - for version and project name in generated AssemblyInfo file
//  - by the generated NuGet package
//  - to run tests and to publish documentation on GitHub gh-pages
//  - for documentation, you also need to edit info in "docs/tools/generate.fsx"

// The name of the project
// (used by attributes in AssemblyInfo, name of a NuGet package and directory in 'src')
let project = "MBrace.AWS"

// Short summary of the project
// (used as description in AssemblyInfo and as a short summary for NuGet package)
let summary = "AWS PaaS bindings for MBrace"

// Longer description of the project
// (used as a description for NuGet package; line breaks are automatically cleaned up)
let description = "AWS PaaS bindings for MBrace"

// List of author names (for NuGet package)
let authors = [ "Yan Cui"; "Eirik Tsarpalis" ]

// Tags for your project (for NuGet package)
let tags = "mbrace fsharp aws"

// File system information 
let solutionFile  = "MBrace.AWS.sln"

// Pattern specifying assemblies to be tested using NUnit
let testAssemblies = ["bin/MBrace.AWS.Tests.dll"]

let gitHash = Information.getCurrentHash()
let buildDate = DateTime.UtcNow
let release = LoadReleaseNotes "RELEASE_NOTES.md"
let isAppVeyorBuild = buildServer = BuildServer.AppVeyor
let isVersionTag tag = Version.TryParse tag |> fst
let hasRepoVersionTag = isAppVeyorBuild && AppVeyorEnvironment.RepoTag && isVersionTag AppVeyorEnvironment.RepoTagName
let assemblyVersion = if hasRepoVersionTag then AppVeyorEnvironment.RepoTagName else release.NugetVersion
let buildVersion =
    if hasRepoVersionTag then assemblyVersion
    else if isAppVeyorBuild then sprintf "%s-b%s" assemblyVersion AppVeyorEnvironment.BuildNumber
    else assemblyVersion

// Git configuration (used for publishing documentation in gh-pages branch)
// The profile where the project is posted
let gitOwner = "mbraceproject" 
let gitHome = "https://github.com/" + gitOwner

// The name of the project on GitHub
let gitName = "MBrace.AWS"

// The url for the raw files hosted
let gitRaw = environVarOrDefault "gitRaw" ("https://raw.github.com/" + gitOwner)

let runVagabondTestsOnly = environVarOrDefault "RunVagabondTestsOnly" "false" |> Boolean.Parse

// --------------------------------------------------------------------------------------
// END TODO: The rest of the file includes standard build steps
// --------------------------------------------------------------------------------------

// Read additional information from the release notes document

Target "BuildVersion" (fun _ ->
    Shell.Exec("appveyor", sprintf "UpdateBuild -Version \"%s\"" buildVersion) |> ignore
)

// Helper active pattern for project types
let (|Fsproj|Csproj|Vbproj|) (projFileName:string) = 
    match projFileName with
    | f when f.EndsWith("fsproj") -> Fsproj
    | f when f.EndsWith("csproj") -> Csproj
    | f when f.EndsWith("vbproj") -> Vbproj
    | _                           -> failwith (sprintf "Project file %s not supported. Unknown project type." projFileName)

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let getAssemblyInfoAttributes projectName =
        [ Attribute.Title (projectName)
          Attribute.Product project
          Attribute.Description summary
          Attribute.Metadata("Release Signature", 
                sprintf "Version %s, Git Hash %s, Build Date %s" 
                    assemblyVersion
                    gitHash 
                    (buildDate.ToString "ddMMyyyy HH:mm zzz"))
          Attribute.Version assemblyVersion
          Attribute.FileVersion assemblyVersion ]

    let getProjectDetails projectPath =
        let projectName = System.IO.Path.GetFileNameWithoutExtension(projectPath)
        ( projectPath, 
          projectName,
          System.IO.Path.GetDirectoryName(projectPath),
          (getAssemblyInfoAttributes projectName)
        )

    !! "src/**/*.??proj"
    |> Seq.map getProjectDetails
    |> Seq.iter (fun (projFileName, projectName, folderName, attributes) ->
        match projFileName with
        | Fsproj -> CreateFSharpAssemblyInfo (folderName @@ "AssemblyInfo.fs") attributes
        | Csproj -> CreateCSharpAssemblyInfo ((folderName @@ "Properties") @@ "AssemblyInfo.cs") attributes
        | Vbproj -> CreateVisualBasicAssemblyInfo ((folderName @@ "My Project") @@ "AssemblyInfo.vb") attributes
        )
)

// --------------------------------------------------------------------------------------
// Clean build results

Target "Clean" (fun _ ->
    CleanDirs ["bin"; "temp"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->
    !! solutionFile
    |> MSBuildRelease "" "Rebuild"
    |> ignore
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner

Target "RunTests" (fun _ ->
    testAssemblies 
    |> Seq.collect (!!)
    |> NUnitSequential.NUnit (fun p ->
        { p with
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 120.
            IncludeCategory = if runVagabondTestsOnly then "Vagabond" else p.IncludeCategory
            OutputFile = "TestResults.xml" })
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    Paket.Pack(fun p -> 
        { p with
            OutputPath = "bin"
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes})
)

Target "PublishNuget" (fun _ ->
    Paket.Push(fun p -> 
        { p with
            WorkingDir = "bin" })
)

// --------------------------------------------------------------------------------------
// Github Releases

#load "paket-files/build/fsharp/FAKE/modules/Octokit/Octokit.fsx"
open Octokit

Target "ReleaseGitHub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.pushBranch "" remote (Information.getBranchName "")

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "OctokitToken" with
        | null -> 
            let user =
                match getBuildParam "github-user" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserInput "Username: "
            let pw =
                match getBuildParam "github-pw" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserPassword "Password: "

            createClient user pw
        | token -> createClientWithToken token

    // release on github
    client
    |> createDraft gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes
    |> releaseDraft
    |> Async.RunSynchronously
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "RunTestsAndBuildNuget" DoNothing
Target "Release" DoNothing


"Clean"
  =?> ("BuildVersion", isAppVeyorBuild)
  ==> "AssemblyInfo"
  ==> "Build"
  =?> ("RunTests", not isAppVeyorBuild) // testing not yet enabled on appveyor because Azure resource access is needed
  ==> "Default"

"NuGet" ==> "RunTestsAndBuildNuget"
"RunTests" ==> "RunTestsAndBuildNuget"

"Build" 
  ==> "NuGet"
  ==> "PublishNuget"
  ==> "ReleaseGithub"
  ==> "Release"

RunTargetOrDefault "Default"