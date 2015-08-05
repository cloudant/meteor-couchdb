# couchdb

The `couchdb` package is a Meteor package available on
[Atmosphere](https://atmospherejs.com/cloudant/couchdb). The package is [full stack database
driver](https://www.meteor.com/full-stack-db-drivers) that provides
functionality to work with Apache CouchDB in Meteor.  

It provides:  

- an efficient [Livequery](https://www.meteor.com/livequery) implementation providing real-time
  updates from the database by consuming the CouchDB _changes feed
- DDP RPC end-points for updating the data from clients connected over the wire
- Serialization and deserialization of updates to the DDP format

This Readme covers the following

* [Installation and Usage](#installation)
* [API Reference](#api-reference)
* [License](#license)


## Installation

Add this package to your Meteor app:

    meteor add cloudant:couchdb

Since Apache CouchDB is not shipped with Meteor or this package, you need to
have a running CouchDB/Cloudant server and a url to connect to it. 

Note: The JSON query syntax used is ['Cloudant Query'](https://cloudant.com/blog/couchdb-and-mongodb-let-our-query-apis-combine/#.VbOqmhOqqko), initially developed by Cloudant and contributed back to Apache CouchDB version 2.0. Pre-built binaries of Apache CouchDB 2.0 are not yet available, so the easiest way to use this module is with Cloudant [DBaas](https://cloudant.com/product/) or [Local](https://cloudant.com/cloudant-local/)

To configure the Apache CouchDB/Cloudant server connection information, pass its url as the `COUCHDB_URL`
environment variable to the Meteor server process. 

    $export COUCHDB_URL=https://username:password@username.cloudant.com 

## Database API

Just like Mongo.Collection, you will work with CouchDB.Database for
CouchDB data.

You can instantiate a CouchDB.Database on both client and on the server.  

```javascript
var Tasks = new CouchDB.Database("tasks");
```

The database wraps the Cloudant Query commands.  If a callback is passed then the
commands execute asynchronously.  If no callback is passed, on the server,
the call is executed synchronously (technically this uses fibers and only
appears to be synchronous, so it does not block the event-loop).  If
you're on the client and don't pass a callback, the call executes asynchronously
and you won't be notified of the result.

## Publish/Subscribe

One can publish a cursor on the server and the client subscribe to it.  

```javascript
if (Meteor.isServer) {
  // This code only runs on the server
  Meteor.publish("tasks", function () {
    return Tasks.find();
  });
}

if (Meteor.isClient) {
  // This code only runs on the client
  Meteor.subscribe("tasks");
}
```

This way data will be automatically synchronized to all subscribed clients.

## Latency compensation

Latency compensation works with all supported commands used either at the
client or client's simulations.

## Permissions: allow/deny

Once you remove the insecure package, you can allow/deny database modifications
from the client

```javascript
//make sure no extra properties besides postContent are included in the insert operation
Tasks.allow({
  insert: function (userId, doc) {
    return _.without(_.keys(doc), 'postContent').length === 0;
  }
})
```  

## API Reference  

* [CouchDB.Database](#couchdb-database)  
	- [database.find](#database-find-selector-options)  
	- [database.findOne](#databasefindoneselectoroptions)  
	- [database.insert](#database-insert-doc-callback)  
	- [database.update](#database-update-doc-options-callback)  
	- [database.upsert](#database-upsert-doc-callback)  
	- [database.remove](#database-remove-id-callback)  
	- [database.allow](#database-allow-options)  
	- [database.deny](#database-deny-options)  
* [CouchDB.Cursor](#couchdb-cursor)  
	- [cursor.forEach](#cursor-foreach-callback-thisarg)  
	- [cursor.map](#cursor-map-callback-thisarg)  
	- [cursor.fetch](#cursor-fetch)  
	- [cursor.count](#cursor-count)  
	- [cursor.observe](#cursor-observe-callbacks)  
	- [cursor.observeChanges](#cursor-observechanges-callbacks)  
* [Query Syntax](#query-syntax)  
	- [Selectors](#selectors)  
	- [Sort specifiers](#sort-specifiers)  
	- [Field Specifiers](#field-specifiers)  
		

## CouchDB.Database

Apache CouchDB stores data in Databases. To get started, declare a database with new CouchDB.Database.

```  
new CouchDB.Database(name, [options])
  Constructor for a Database

  Arguments
    name String
    The name of the database. If null, creates an unmanaged (unsynchronized) local database.

  Options
    connection Object
    The server connection that will manage this database. Uses the default connection if not specified. Pass the return value of calling   DDP.connect to specify a different server. Pass null to specify no connection. Unmanaged (name is null) databases cannot specify a connection.

    idGeneration String
    The method of generating the _id fields of new documents in this database. Possible values:

     'STRING': random strings
     The default id generation technique is 'STRING'.  
```  

Calling this function sets up a database (a storage space for records, or "documents") that can be used to store a particular type of information that matters to your application. Each document is a JSON object. It includes an _id property whose value is unique in the database, which Meteor will set when you first create the document.  



```  
// common code on client and server declares a DDP-managed couchdb
// database.
Chatrooms = new CouchDB.Database("chatrooms");  
Messages = new CouchDB.Database("messages");  
```  

The function returns an object with methods to insert documents in the database, update their properties, and remove them, and to find the documents in the database that match arbitrary criteria. The way these methods work is compatible with the popular CouchDB JSON Query syntax. The same database API works on both the client and the server (see below).  


```  
// return array of my messages
var myMessages = Messages.find({userId: Session.get('myUserId')}).fetch();

// create a new message
var id = Messages.insert({text: "Hello, world!"});

// mark my first message as "important"
Messages.update({_id: id, text: 'Hello, world!', important: true });
```  

If you pass a name when you create the database, then you are declaring a persistent database — one that is stored on the server and seen by all users. Client code and server code can both access the same database using the same API.

Specifically, when you pass a name, here's what happens:

On the server (if you do not specify a connection), a database with that name is created on the backend CouchDB server. When you call methods on that database on the server, they translate directly into normal CouchDB operations (after checking that they match your access control rules).

On the client (and on the server if you specify a connection), Meteor's [Minimongo](https://www.meteor.com/mini-databases) is reused i.e. Minimongo instance is created. Queries (find) on these databases are served directly out of this cache, without talking to the server.

When you write to the database on the client (insert, update, remove), the command is executed locally immediately, and, simultaneously, it's sent to the server and executed there too. This happens via stubs, because writes are implemented as methods.

When, on the server, you write to a database which has a specified connection to another server, it sends the corresponding method to the other server and receives the changed values back from it over DDP. Unlike on the client, it does not execute the write locally first.

If you pass null as the name, then you're creating a local database. It's not synchronized anywhere; it's just a local scratchpad that supports find, insert, update, and remove operations. (On both the client and the server, this scratchpad is implemented using Minimongo.)
  
  
### database.find([selector],[options])
  

Find the documents in a database that match the selector.

> Arguments  
>>selector : [Cloudant Selector](#selectors),  or String  
>>>A query describing the documents to find

>>options
>>>sort [Cloudant Sort](#sort-specifiers) Specifier
>>>>Sort order 

>>> skip Number
>>>>Number of results to skip at the beginning

>>>limit Number
>>>>Maximum number of results to return

>>>fields : [Cloudant Field](#field-specifiers) Specifier
>>>>fields to return.  
 

find returns a cursor. It does not immediately access the database or return documents. Cursors provide fetch to return all matching documents, map and forEach to iterate over all matching documents, and observe and observeChanges to register callbacks when the set of matching documents changes.

Cursors are not query snapshots. Cursors are a reactive data source. Any change to the database that changes the documents in a cursor will trigger a recomputation. 

### database.findOne([selector],[options])  

  Finds the first document that matches the selector, as ordered by sort and skip options.

> Arguments  
>>selector : [Cloudant Selector](#selectors),  or String  
>>>A query describing the documents to find

>>Options
>>>sort [Cloudant Sort](#sort-specifiers) Specifier
>>>>Sort order 

>>> skip Number
>>>>Number of results to skip at the beginning

>>>limit Number
>>>>Maximum number of results to return

>>>fields : [Cloudant Field](#field-specifiers) Specifier
>>>>fields to return.  

Equivalent to find(selector, options).fetch()[0] with options.limit = 1.

### database.insert(doc,[callback])  

```  
  Insert a document in the database. Returns its unique _id.

  Arguments
    doc Object
      The document to insert. May not yet have an _id attribute, in which case Meteor will generate one for you.

    callback Function
     Optional. If present, called with an error object as the first argument and, if no error, the _id as the second.  
```  

Add a document to the database. A document is just an object, and its fields can contain any combination of compatible datatypes (arrays, objects, numbers, strings, null, true, and false).

insert will generate a unique ID for the object you pass, insert it in the database, and return the ID. 

### database.update(doc,[options],[callback])  

```  
  Modify a document in the database. Returns 1 if document updated, 0 if not.

  Arguments
    doc JSON document with _id field
      the _id field in this doc specifies which document in the database is to be replaced by this document's content.

    options
      upsert Boolean
        True to insert a document if no matching document is found.

    callback Function
      Optional. If present, called with an error object as the first argument and, if no error, returns 1 as the second.

```  

Replace a document that matches the _id field. This is done on the Apache CouchDB Server via a updateHandler ignoring the _rev field (Hence behaviour is same as last-writer-wins)

Returns 1 from the update call if successful and you don't pass a callback.

You can use update to perform a upsert by setting the upsert option to true. You can also use the upsert method to perform an upsert that returns the _id of the document that was inserted (if there was one)  

### database.upsert(doc,[callback])

```  
  Replace a document in the database, or insert one if no matching document were found. Returns an object with keys numberAffected (1 if successful, otherwise 0) and insertedId (the unique _id of the document that was inserted, if any).

  Arguments
    doc JSON document with _id field
      the _id field in this doc specifies which document in the database is to be replaced by this document's content if exists. If doesnt exist document is inserted

    callback Function
      Optional. If present, called with an error object as the first argument and, if no error, returns 1 as the second.
```  

Replace a document that matches the _id of the document, or insert a document if no document matched the _id. This is done on the Apache CouchDB Server via a updateHandler ignoring the _rev field (hence behaviour is same as last-writer-wins). upsert is the same as calling update with the upsert option set to true, except that the return value of upsert is an object that contain the keys numberAffected and insertedId. (update returns only 1 if successful or 0 if not)

### database.remove(id,[callback])  

```  
  Remove a document from the database.
  
  Arguments
    id 
      _id value of the document to be removed

    callback Function
      Optional. If present, called with an error object as the first argument and, if no error, returns 1 as the second.
  
```  

Delete the document whose _id matches the specified value them from the database. This is done on the Apache CouchDB Server via a updateHandler ignoring the _rev field 

1 will be returned  when successful otherwise 0, if you don't pass a callback. 

### database.allow(options)

```  
  Allow users to write directly to this database from client code, subject to limitations you define.

  options
    insert, update, remove Function
      Functions that look at a proposed modification to the database and return true if it should be allowed.
```  
When a client calls insert, update, or remove on a database, the database's allow and deny callbacks are called on the server to determine if the write should be allowed. If at least one allow callback allows the write, and no deny callbacks deny the write, then the write is allowed to proceed.

These checks are run only when a client tries to write to the database directly, for example by calling update from inside an event handler. Server code is trusted and isn't subject to allow and deny restrictions. That includes methods that are called with Meteor.call — they are expected to do their own access checking rather than relying on allow and deny.

You can call allow as many times as you like, and each call can include any combination of insert, update, and remove functions. The functions should return true if they think the operation should be allowed. Otherwise they should return false, or nothing at all (undefined). In that case Meteor will continue searching through any other allow rules on the database.

The available callbacks are:

- insert(userId, doc)  
  The user userId wants to insert the document doc into the database. Return true if this should be allowed. doc will contain the _id field if one was explicitly set by the client. You can use this to prevent users from specifying arbitrary _id fields.

- update(userId, doc, fieldNames)
  The user userId wants to update a document doc. (doc is the current version of the document from the database, without the proposed update.) Return true to permit the change. fieldNames is an array of the (top-level) fields in doc that the client wants to modify, for example ['name', 'score'].

- remove(userId, doc)
  The user userId wants to remove doc from the database. Return true to permit this.

When calling update or remove Meteor will by default fetch the entire document doc from the database. If you have large documents you may wish to fetch only the fields that are actually used by your functions. Accomplish this by setting fetch to an array of field names to retrieve.

If you never set up any allow rules on a database then all client writes to the database will be denied, and it will only be possible to write to the database from server-side code. In this case you will have to create a method for each possible write that clients are allowed to do. You'll then call these methods with Meteor.call rather than having the clients call insert, update, and remove directly on the database.

### database.deny(options)  

```  
  Override allow rules.

  options
    insert, update, remove Function
      Functions that look at a proposed modification to the database and return true if it should be denied, even if an allow rule says otherwise.  
  
```  

This works just like allow, except it lets you make sure that certain writes are definitely denied, even if there is an allow rule that says that they should be permitted.

When a client tries to write to a database, the Meteor server first checks the database's deny rules. If none of them return true then it checks the database's allow rules. Meteor allows the write only if no deny rules return true and at least one allow rule returns true.  

## CouchDB.Cursor

To create a cursor, use database.find. To access the documents in a cursor, use forEach, map, or fetch.

### cursor.forEach(callback,[thisArg])  

```  
  Call callback once for each matching document, sequentially and synchronously.

  Arguments
    callback Function
      Function to call. It will be called with three arguments: the document, a 0-based index, and cursor itself.

    thisArg Any
      An object which will be the value of this inside callback.  
      
```  
When called from a reactive computation, forEach registers dependencies on the matching documents.  


### cursor.map(callback,[thisArg])  

```  
  Map callback over all matching documents. Returns an Array.

  Arguments
    callback Function
      Function to call. It will be called with three arguments: the document, a 0-based index, and cursor itself.

    thisArg Any
      An object which will be the value of this inside callback.  
      
``` 
When called from a reactive computation, map registers dependencies on the matching documents.

On the server, if callback yields, other calls to callback may occur while the first call is waiting. If strict sequential execution is necessary, use forEach instead.  


### cursor.fetch()  

```  
  Return all matching documents as an Array.
     
``` 
When called from a reactive computation, fetch registers dependencies on the matching documents.  


### cursor.count()  

```  
  Returns the number of documents that match a query.
     
``` 
Unlike the other functions, count registers a dependency only on the number of matching documents. (Updates that just change or reorder the documents in the result set will not trigger a recomputation.)

### cursor.observe(callbacks)  

```  
  Watch a query. Receive callbacks as the result set changes.

  Arguments
    callbacks Object
      Functions to call to deliver the result set as it changes
     
``` 
This follow same behaviour of [mongo-livedata driver](http://docs.meteor.com/#/full/observe)


### cursor.observeChanges(callbacks)  

```  
  Watch a query. Receive callbacks as the result set changes. Only the differences between the old and new documents are passed to the callbacks.

  Arguments
    callbacks Object
      Functions to call to deliver the result set as it changes     
``` 
This follow same behaviour of [mongo-livedata driver](http://docs.meteor.com/#/full/observe_changes)


## Query Syntax  

Uses Cloudant query syntax.  

### Selectors 

The simplest selectors are just a string. These selectors match the document with that value in its _id field.

A slightly more complex form of selector is an object containing a set of keys that must match in a document:

```  
// Matches all documents where the name and cognomen are as given  
{name: "Rhialto", cognomen: "the Marvelous"}

// Matches every document  
{}  
```  

But they can also contain more complicated tests:

```  
// Matches documents where age is greater than 18  
{age: {$gt: 18}}  
```  

See the [complete documentation](https://docs.cloudant.com/cloudant_query.html#selector-syntax)  

### Sort specifiers 

Sorts maybe specified using the Cloudant sort syntax

[{"fieldName1": "desc"}, {"fieldName2": "desc" }]

```  
//Example  
[{"Actor_name": "asc"}, {"Movie_runtime": "desc"}]  
```  

See the [complete documentation](https://docs.cloudant.com/cloudant_query.html#sort-syntax)  

### Field specifiers 

JSON array following the field syntax, described below. This parameter lets you specify which fields of an object should be returned. If it is omitted, the entire object is returned.

```  
// Example include only Actor_name, Movie_year and _id  
["Actor_name", "Movie_year", "_id"]  
```  


See the [complete documentation](https://docs.cloudant.com/cloudant_query.html#filtering-fields)  



# License

Copyright (c) 2015 IBM Corporation

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and/or sell copies of the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION 
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

   
