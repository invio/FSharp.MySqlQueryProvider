module FSharp.MySqlQueryProvider.Queryable

open MySql.Data.MySqlClient
open FSharp.MySqlQueryProvider

open System.Reflection
open System.Linq
open System.Linq.Expressions
open System.Collections
open System.Collections.Generic
open System.Runtime.InteropServices

module internal TypeSystem = 
   
    let implements implementerType (interfaceType : System.Type) =
        interfaceType.IsAssignableFrom(implementerType)

    let rec findIEnumerable seqType = 
        if seqType = null || seqType = typedefof<string> then
            None
        else
            if seqType.IsArray then
                Some (typedefof<IEnumerable<_>>.MakeGenericType(seqType.GetElementType()))
            else 
                let assigneable =
                    match seqType.GetTypeInfo().IsGenericType with
                    | true ->
                        seqType.GetGenericArguments()
                        |> Seq.tryFind(fun (arg) -> 
                            typedefof<IEnumerable<_>>.MakeGenericType(arg).IsAssignableFrom(seqType))
                    | false -> None
                if assigneable.IsSome then
                    Some assigneable.Value
                else
                    let iface =
                        let ifaces = seqType.GetInterfaces()
                        if (ifaces <> null) && ifaces.Length > 0 then
                            ifaces |> Seq.tryPick(findIEnumerable)
                        else 
                            None
                    if iface.IsSome then
                        iface
                    else
                        let seqTypeInfo = seqType.GetTypeInfo()
                        if seqTypeInfo.BaseType <> null && seqTypeInfo.BaseType <> typedefof<obj> then
                            findIEnumerable(seqTypeInfo.BaseType)
                        else 
                            None

    let getElementType seqType =
        let ienum = findIEnumerable(seqType)
        match ienum with 
        | None -> seqType
        | Some(ienum) ->  ienum
            //ienum.GetGenericArguments() |> Seq.head

/// <summary>
/// Type to remove boiler plate when implementing IQueryProvider
/// </summary>
type Query<'T>(provider : QueryProvider,
               [<Optional; DefaultParameterValue(null)>] expression : Expression option) as this =

    let getDefaultCallExpression() =
        let flags = BindingFlags.NonPublic ||| BindingFlags.Static
        let genericType = (typedefof<Query<_>>).MakeGenericType(typedefof<'T>)
        let methodInfo = genericType.GetMethod("nothing", flags)
        Expression.Call(null, methodInfo, Expression.Constant(this)) :> Expression

    let hardExpression =
        match expression with
        | Some x -> x
        | None -> getDefaultCallExpression()

    let mutable result : IEnumerable<'T> option = None
    let resultLock = obj()

    static let nothing (queryable: IQueryable<'T>) = queryable
    member __.provider = provider
    member __.expression = hardExpression
    
    member private this.getEnumerable() = 
        lock resultLock (fun () ->
            match result with
            | None ->
                let res = this.provider.Execute(hardExpression) :?> IEnumerable<'T>
                result <- Some res
                res
            | Some result -> 
                result)

    interface IQueryable<'T> with
        member __.Expression =
            hardExpression
        
        member __.ElementType =
            typedefof<'T>
    
        member this.Provider = 
            this.provider :> IQueryProvider

    interface IOrderedQueryable<'T>
        
    interface IEnumerable with
        member this.GetEnumerator() =
            (this.getEnumerable() :> IEnumerable).GetEnumerator()

    interface IEnumerable<'T> with
        member this.GetEnumerator() =
            this.getEnumerable().GetEnumerator()

    override __.ToString () =
        match expression with 
        | Some e -> e.ToString()
        | None -> sprintf "value(Query<%s>)" typedefof<'T>.Name

/// <summary>
/// Type to remove boiler plate when implementing IQueryProvider
/// </summary>
and [<AbstractClass>] QueryProvider() =

    interface IQueryProvider with 
        
        member this.CreateQuery<'S> (expression : Expression) = 
            let q = Query<'S>(this, Some expression) :> IQueryable<'S>
            q
        member this.CreateQuery (expression : Expression) = 
            let elementType = TypeSystem.getElementType(expression.Type)
            try
                let generic = typedefof<Query<_>>.MakeGenericType(elementType)

                let args : obj array = [|this; Some expression|]
                let instance = System.Activator.CreateInstance(generic, args)
                instance :?> IQueryable
            with 
            | :? TargetInvocationException as ex -> raise ex.InnerException
        
        member this.Execute<'S> expression =
            this.Execute expression :?> 'S

        member this.Execute (expression) =
            this.Execute expression

    abstract member Execute : Expression -> obj

let makeQuery<'t> queryProvider =
    Query<'t>(queryProvider, None) :> System.Linq.IQueryable<'t>
