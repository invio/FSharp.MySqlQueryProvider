### 1.0.9 - February 11 2018
* Upgrade Invio.Extensions.Reflection to v1.0.8

### 1.0.8 - May 17 2017
* Adding bitwise operation support for &, |, ^.
* Adding another test for Empty List with the way around an empty list.

### 1.0.7 - May 16 2017
* Adding query text in exception message when an execute fails.
* Adding fixing SQL for when no items are in an IN statement and just using FALSE in it's place.

### 1.0.6 - April 17 2017
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
