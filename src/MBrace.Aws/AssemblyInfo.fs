namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace.Aws")>]
[<assembly: AssemblyProductAttribute("MBrace.Aws")>]
[<assembly: AssemblyDescriptionAttribute("AWS backend for MBrace")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
