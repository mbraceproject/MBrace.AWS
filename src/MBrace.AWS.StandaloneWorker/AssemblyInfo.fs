namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace.AWS.StandaloneWorker")>]
[<assembly: AssemblyProductAttribute("MBrace.AWS")>]
[<assembly: AssemblyDescriptionAttribute("AWS PaaS bindings for MBrace")>]
[<assembly: AssemblyVersionAttribute("0.1.4")>]
[<assembly: AssemblyFileVersionAttribute("0.1.4")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.1.4"
    let [<Literal>] InformationalVersion = "0.1.4"
