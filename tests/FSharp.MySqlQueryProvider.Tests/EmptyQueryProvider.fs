﻿module EmptyQueryProvider
open FSharp.MySqlQueryProvider.Queryable

type EmptyQueryProvider() =
    inherit QueryProvider()

    let mutable expressions = Seq.empty<System.Linq.Expressions.Expression>

    member this.Expressions with get() = expressions
    override this.Execute expression =
        expressions <- ([expression] |> Seq.append expressions)
        failwith "EmptyQueryProvider.Execute not implemented"
