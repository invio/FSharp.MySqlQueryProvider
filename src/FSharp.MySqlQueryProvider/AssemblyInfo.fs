namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSharp.MySqlQueryProvider")>]
[<assembly: AssemblyProductAttribute("FSharp.MySqlQueryProvider")>]
[<assembly: AssemblyDescriptionAttribute("Implements IQueryable and IQueryProvider for MySql in F# functional and easy.")>]
[<assembly: AssemblyVersionAttribute("1.0.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.0"
