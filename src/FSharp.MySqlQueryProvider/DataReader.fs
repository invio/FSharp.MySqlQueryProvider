module FSharp.MySqlQueryProvider.DataReader

open System
open System.Collections
open System.Linq
open System.Reflection
open Microsoft.FSharp.Reflection
open Invio.Extensions.Reflection

type ReturnType = 
| Single
| SingleOrDefault
| Many

type TypeOrLambdaConstructionInfo = 
| Type of TypeConstructionInfo
| Lambda of LambdaConstructionInfo

and TypeOrValueOrLambdaConstructionInfo = 
| Type of TypeConstructionInfo
| Value of int
| Bool of int
| Enum of EnumConstructionInfo
| Lambda of LambdaConstructionInfo

and TypeConstructionInfo = {
    Type : System.Type
    ConstructorArgs : TypeOrValueOrLambdaConstructionInfo seq
    PropertySets : (TypeOrValueOrLambdaConstructionInfo * System.Reflection.PropertyInfo) seq
}

and LambdaConstructionInfo = {
    Lambda : System.Linq.Expressions.LambdaExpression
    Parameters : TypeOrValueOrLambdaConstructionInfo seq
}

and EnumConstructionInfo = {
    Type : System.Type
    Index : int
}

type ConstructionInfo = {
    ReturnType : ReturnType
    Type : System.Type
    TypeOrLambda : TypeOrLambdaConstructionInfo
    PostProcess : System.Linq.Expressions.LambdaExpression option
}

let createTypeConstructionInfo t constructorArgs propertySets =
    {
        Type = t
        ConstructorArgs = constructorArgs
        PropertySets = propertySets
    }

let isValueType (t : System.Type) =
    let ti = t.GetTypeInfo()
    ti.IsValueType || t = typedefof<string>
let isOption (t : System.Type) = 
    let ti = t.GetTypeInfo()
    ti.IsGenericType &&
    ti.GetGenericTypeDefinition() = typedefof<Option<_>>
let isNullable (t : System.Type) = 
    let ti = t.GetTypeInfo()
    ti.IsGenericType &&
    ti.GetGenericTypeDefinition() = typedefof<Nullable<_>>
let isEnumType (t : System.Type) =
    let ti = t.GetTypeInfo()
    ti.IsEnum

let readBool (value : obj) = 
    match value with 
    | :? int as i -> 
        match i with 
        | 0 -> false :> obj
        | 1 -> true :> obj
        | i -> failwithf "not a bit %i" i
    | :? bool as b -> b :> obj
    | x -> failwithf "unexpected type %s" (x.GetType().FullName)

let rec constructResult (reader : System.Data.IDataReader) (ctor : ConstructionInfo) : obj =
    
    match ctor.TypeOrLambda with
    | TypeOrLambdaConstructionInfo.Lambda lambdaCtor ->  
        invokeLambda reader lambdaCtor
    | TypeOrLambdaConstructionInfo.Type typeCtor ->
        constructType reader typeCtor

and invokeLambda reader lambdaCtor = 
    let paramValues = 
        lambdaCtor.Parameters 
        |> Seq.map(fun p -> 
            match p with
            | TypeOrValueOrLambdaConstructionInfo.Type typeCtor -> constructType reader typeCtor
            | TypeOrValueOrLambdaConstructionInfo.Lambda lambdaCtor -> invokeLambda reader lambdaCtor
            | TypeOrValueOrLambdaConstructionInfo.Value i -> reader.GetValue i
            | TypeOrValueOrLambdaConstructionInfo.Bool i -> readBool ((reader.GetValue i))
            | TypeOrValueOrLambdaConstructionInfo.Enum enumCtor -> constructEnum reader enumCtor)
    lambdaCtor.Lambda.Compile().DynamicInvoke(paramValues |> Seq.toArray)

and constructEnum reader enumCtor =
    Enum.ToObject(enumCtor.Type, (reader.GetValue enumCtor.Index))

and constructType reader typeCtor = 
    let getSingleIndex() = 
        match typeCtor.ConstructorArgs |> Seq.exactlyOne with
        | Type _ -> failwith "Shouldn't be Type"
        | Lambda _ -> failwith "Shouldn't be Lambda"
        | Enum e -> e.Index
        | Bool i | Value i -> i

    let getValue i = 
        let typeName = reader.GetDataTypeName i
        let value = reader.GetValue i 
        if typeName = "char" then
            let str = value :?> string 
            str.TrimEnd() :> obj
        else
            value

    let t = typeCtor.Type
    let ti = t.GetTypeInfo()
    if t = typedefof<string> ||
        t = typedefof<byte> ||
        t = typedefof<sbyte> ||
        t = typedefof<char> ||
        t = typedefof<System.DateTime> ||
        t = typedefof<decimal> ||
        t = typedefof<double> ||
        t = typedefof<float> ||
        t = typedefof<System.Guid> ||
        t = typedefof<uint16> ||
        t = typedefof<uint32> ||
        t = typedefof<uint64> ||
        t = typedefof<int16> ||
        t = typedefof<int32> ||
        t = typedefof<int64> then
        Convert.ChangeType((getValue (getSingleIndex())), t)
    else if t = typedefof<bool> then
        getValue (getSingleIndex()) |> readBool 
    else if ti.IsEnum then
        getValue (getSingleIndex())
    else if isNullable t then
        let i = getSingleIndex()
        if reader.IsDBNull(i) then
            null :> obj
        else
            let value =
                match typeCtor.ConstructorArgs |> Seq.exactlyOne with 
                    | Type t -> failwith "Shouldn't be Type"
                    | Lambda l -> failwith "Shouldn't be Lambda"
                    | Enum e -> constructEnum reader e
                    | Bool i -> getValue i |> readBool
                    | Value i -> getValue i
            if value <> null then
                value
            else
                null :> obj
    else if isOption t then
        let i = getSingleIndex()
        if reader.IsDBNull(i) then
            None :> obj
        else
            let value = reader.GetValue (i)
            if value <> null then
                ti.GetMethod("Some").Invoke(null, [| value |])
            else
                None :> obj
    else
        let getCtorArgs () =  
            typeCtor.ConstructorArgs
            |> Seq.map(fun arg -> 
                match arg with 
                | Type t -> constructType reader t
                | Lambda l -> invokeLambda reader l
                | Enum e -> constructEnum reader e
                | Bool i -> i |> getValue |> readBool
                | Value i -> getValue i)
            |> Seq.toArray

        let inst = 
            if FSharpType.IsRecord t then
                try
                    FSharpValue.MakeRecord(t, getCtorArgs())
                with
                | ex ->
                    let wrongTypeFields =
                        FSharpType.GetRecordFields t
                        |> Seq.mapi(fun i x -> 
                            let returnType = reader.GetFieldType i
                            let expectedType =
                                if x.PropertyType.GetTypeInfo().IsEnum then
                                    typedefof<int>
                                else
                                    x.PropertyType
                            returnType, expectedType, x)
                        |> Seq.filter(fun (rt, et, _) ->
                            rt <> et)
                    
                    let fieldMessages = 
                        wrongTypeFields 
                        |> Seq.map(fun (rt, et, recField) ->
                            sprintf "Field \"%s\" expected type \"%s\" but was \"%s\"" recField.Name
                                                                                       et.FullName
                                                                                       rt.FullName)
                        |> String.concat "\n"
                    let message = 
                        "Exception initializing record, types did not match:\n" + 
                        fieldMessages
                    let newEx = System.Exception(message, ex)
                    raise newEx
            else
                let args = getCtorArgs()
                let ctor = t.GetConstructors().Single()
                let create = ctor.CreateArrayFunc()
                create.Invoke(args)
        if typeCtor.PropertySets |> Seq.length > 0 then
            failwith "PropertySets are not implemented"
        
        inst

let private moreThanOneMessage = "Sequence contains more than one element"
let private noElementsMessage = "Sequence contains no elements"

let read (reader : System.Data.IDataReader) constructionInfo : obj = 
    let constructResult () = 
        constructResult reader constructionInfo
         
    let returnType = constructionInfo.ReturnType
    let t = constructionInfo.Type
    let getAll() = 
        let listT = typedefof<System.Collections.Generic.List<_>>
        let conListT = listT.MakeGenericType([| t |])
        let conListTInfo = conListT.GetTypeInfo()
        let addM = conListTInfo.GetMethods() |> Seq.find(fun m -> m.Name = "Add")
        let inst = System.Activator.CreateInstance(conListT)
        while reader.Read() do
            let res = constructResult()
            addM.Invoke(inst, [|res|]) |> ignore
        inst
    match returnType with
    | Many -> 
        let inst = getAll()
        match constructionInfo.PostProcess with
        | Some postProcess -> postProcess.Compile().DynamicInvoke([|inst|])
        | None -> inst
    | Single | SingleOrDefault ->
        match constructionInfo.PostProcess with
        | Some postProcess -> postProcess.Compile().DynamicInvoke([|getAll()|])
        | None ->
            if reader.Read() then
                let r = constructResult()
                if reader.Read() then
                    raise (System.InvalidOperationException moreThanOneMessage)
                r
            else
                match returnType with
                | Single -> raise (System.InvalidOperationException noElementsMessage)
                | SingleOrDefault -> 
                    if isValueType t then
                        System.Activator.CreateInstance(t)
                    else
                        null
                | _ -> failwith "shouldn't be here"
