namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace.Aws")>]
[<assembly: AssemblyProductAttribute("MBrace.Aws")>]
[<assembly: AssemblyDescriptionAttribute("AWS backend for MBrace")>]
[<assembly: AssemblyVersionAttribute("0.0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1"
