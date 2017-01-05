[![Build status](https://ci.appveyor.com/api/projects/status/vysigy7jjtadc9rw/branch/master?svg=true)](https://ci.appveyor.com/project/invio/fsharp-mysqlqueryprovider/branch/master)
[![NuGet](https://img.shields.io/nuget/v/FSharp.MySqlQueryProvider.svg)](https://www.nuget.org/packages/FSharp.MySqlQueryProvider/)

**This Project does not implement all possibilities for IQueryProvider and should be tested for use cases not in the MySqlTest.fs.**

# FSharp.MySqlQueryProvider

This project is a fork from FSharp.QueryProvider. The goal is to create a mysql query generation
and object instantiation library that will work with F# Record Types and C# Types which have a single constructor with the initialization of the object in the constructor.

Include from nuget to start using: https://www.nuget.org/packages/FSharp.MySqlQueryProvider/

Example in F#:
```fsharp
open FSharp.MySqlQueryProvider.QueryTranslator
open FSharp.MySqlQueryProvider.Queryable
open MySql.Data.MySqlClient

let connectionString = "Server=localhost;Port=3306;Database=Northwind;Uid=root;Password=password;"
let connection = new MySqlConnection(connectionString)
let queryProvider = new MySqlQueryProvider(connection)

type JobKind =
| Salesman = 0
| Manager = 1

type Person =
    { PersonId : int
      PersonName : string
      JobKind : JobKind
      VersionNo : int }

let does = query {
    for p in makeQuery<Person>(queryProvider) do
    where(p.PersonName.Contains "doe")
    select p
}

printfn "%A" (does |> Seq.toList)
```

Example in C#:
```csharp
using FSharp.MySqlQueryProvider;
using MySql.Data.MySqlClient;

    public enum JobKind {
        Salesman = 0,
        Manager = 1
    }

    public sealed class Person {

        public Int32 PersonId { get; }
        public String PersonName { get; }
        public JobKind JobKind { get; }
        public Int32 VersionNo { get; }

        public Site(
            Int32 personId = default(Int32),
            String personName = default(String),
            JobKind jobKind = default(JobKind),
            Int32 versionNo = default(Int32)) {

            this.PersonId = personId;
            this.PersonName = personName;
            this.JobKind = jobKind;
            this.VersionNo = versionNo;
        }

        public Person SetPersonId(Int32 personId) {
            return new Person(
                personId,
                this.PersonName,
                this.JobKind,
                this.VersionNo
            );
        }

        public Person SetPersonName(String personName) {
            return new Person(
                this.PersonId,
                personName,
                this.JobKind,
                this.VersionNo
            );
        }

        public Person SetJobKind(JobKind jobKind) {
            return new Person(
                this.PersonId,
                this.PersonName,
                jobKind,
                this.VersionNo
            );
        }

        public Person SetVersionNo(Int32 versionNo) {
            return new Person(
                this.PersonId,
                this.PersonName,
                this.JobKind,
                versionNo
            );
        }
    }


    public class MySqlRepository<T> {
        private MySqlConnection connection { get; }
        private MySqlQueryProvider queryProvider { get; }
        
        private const String connectionString = 
            "Server=localhost;Port=3306;Database=Northwind;Uid=root;Password=password;";

        pubic MySqlRepository() {
            this.connection = new MySqlConnection(connectionString);
            this.queryProvider = new MySqlQueryProvider(connection);
        }

        public IQueryable<Person> Queryable() {
            return new FSharp.MySqlQueryProvider.Queryable.Query<T>(this.queryProvider);
        }
    }
    
    // This is how you would finally execute it all together:
    var repository = new MySqlRepository<Person>();
    var queryable = repository.Queryable();
    queryable = queryable.Where(p => p.PersonName.Contains("doe"));
    var result = queryable.ToList();
    
```


## Contributing or Forking

In order to build and run tests run

    $ cd tests/FSharp.MySqlQueryProvider.Tests
    $ dotnet test

## Maintainer(s)

- [@rokadias](https://github.com/rokadias)

## Thanks
- Thanks to [@xavierzwirtz](https://github.com/xavierzwirtz) for his work on building the initial QueryProvider.
- Thanks to [@mattwarren](https://github.com/mattwarren) for his [awesome blog series](http://blogs.msdn.com/b/mattwar/archive/2008/11/18/linq-links.aspx) on writting a reusable LINQ Provider.
