namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace.AWS.StandaloneWorker")>]
[<assembly: AssemblyProductAttribute("MBrace.AWS")>]
[<assembly: AssemblyDescriptionAttribute("AWS backend for MBrace")>]
[<assembly: AssemblyVersionAttribute("0.0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1"
    let [<Literal>] InformationalVersion = "0.0.1"
