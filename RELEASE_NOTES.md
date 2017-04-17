### 1.0.6 - January 05 2017
* Updating to Net Core 1.1.
* Fixing exists clause and csharp any statement by fixing up readBool.

### 1.0.5 - January 05 2017
* Adding skip and take to the query translator along with including filtering for C# methods of Single SingleOrDefault First FirstOrDefault.

### 1.0.4 - January 05 2017
* Setting DateTimeKind to Utc for dates retrieved from the database.

### 1.0.3 - November 29 2016
* Adding special case for string which can be null, and have type dbnull.

### 1.0.2 - November 28 2016
* Updating version of Invio.Extensions.Reflection.

### 1.0.1 - November 28 2016
* Adding check for multiple declarations of functions that can only be called once.

### 1.0.0 - November 28 2016
* Initial release after Converting to MySqlQueryProvider
