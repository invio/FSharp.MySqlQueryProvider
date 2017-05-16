namespace FSharp.MySqlQueryProvider

open MySql.Data.MySqlClient
open System.Linq.Expressions
open FSharp.MySqlQueryProvider
open FSharp.MySqlQueryProvider.Queryable
open FSharp.MySqlQueryProvider.QueryTranslator

/// <summary>
/// IQueryProvider for IDbConnection.
/// </summary>
/// <param name="getConnection">function to get an unopened IDbConnection</param> 
/// <param name="getConnection">function that transforms a IDbConnection and Linq.Expression into a
/// fully constructed IDbCommand and DataReader.ConstructionInfo </param> 
/// <param name="onExecutingCommand">function fired right before executing the reader. The return `obj` is passed into `onExecutedCommand`</param>
/// <param name="onExecutedCommand">function fired right after executing the reader.</param>
type MySqlQueryProvider ( connection : MySqlConnection ) =
    inherit QueryProvider()

    let translate con expression =
        let command, ctor =
            translateToCommand
                QueryDialect.MySQL57
                SelectQuery
                None
                None
                None
                con
                expression

        let ctor =
            match ctor with
            | Some ctor -> ctor
            | None -> failwith "no ctorinfor generated"
        command :> System.Data.IDbCommand, ctor
    
    override __.Execute expression =
        let cmd, ctorInfo =
            try
                translate connection expression
            with
            | e ->
                printfn "Exception during translate: %s" (e.ToString())
                reraise ()

        try
            use reader = cmd.ExecuteReader()
            let res = DataReader.read reader ctorInfo
            res
        with 
        | e -> 
            printfn "Exception during query: %s \nExecute: %s" cmd.CommandText (e.ToString())
            reraise ()

/// <summary>
/// Reusable IQueryProvider for IDbConnection.
/// </summary>
/// <param name="getConnection">function to get an unopened IDbConnection</param> 
/// <param name="getConnection">function that transforms a IDbConnection and Linq.Expression into a
/// fully constructed IDbCommand and DataReader.ConstructionInfo </param> 
/// <param name="onExecutingCommand">function fired right before executing the reader. The return `obj` is passed into `onExecutedCommand`</param>
/// <param name="onExecutedCommand">function fired right after executing the reader.</param>
type DBQueryProvider<'T when 'T :> System.Data.IDbConnection>
    (
    getConnection : unit -> 'T, 
    translate : 'T -> Expression -> System.Data.IDbCommand * DataReader.ConstructionInfo,
    onExecutingCommand : option<System.Data.IDbCommand -> System.Data.IDbCommand * obj>,
    onExecutedCommand : option<System.Data.IDbCommand * obj -> unit>
    ) =
    inherit QueryProvider()
    
    override __.Execute expression =
        try
            use connection = getConnection()
            let cmd, ctorInfo = translate connection expression

            let cmd, userState = 
                match onExecutingCommand with
                | None -> cmd, null
                | Some x -> x cmd

            connection.Open()
            use reader = cmd.ExecuteReader()
            let res = DataReader.read reader ctorInfo
            if onExecutedCommand.IsSome then onExecutedCommand.Value(cmd, userState)
            res
        with 
        | e -> 
            printfn "Exception during query Execute: %s" (e.ToString())
            reraise ()

module foo =
    [<EntryPoint>]
    let main _ =
        0
