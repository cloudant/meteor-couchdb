/**
 * Copyright (c) 2015 IBM Corporation
 * Copyright (C) 2011--2015 Meteor Development Group
 *
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 *  without limitation the rights to use, copy, modify, merge, publish, 
 *  distribute, sublicense, and/or sell copies of the Software, and to 
 *  permit persons to whom the Software is furnished to do so, 
 *  subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION 
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */


/**
 * Provide a synchronous Collection API using fibers, backed by
 * CouchDB.  This is only for use on the server, and mostly identical
 * to the client API.
 *
 * NOTE: the public API methods must be run within a fiber. If you call
 * these outside of a fiber they will explode!
 */

var path = Npm.require('path');
var Cloudant = Npm.require('cloudant');
var Fiber = Npm.require('fibers');
var Future = Npm.require(path.join('fibers', 'future'));

CouchDBInternals = {};
CouchDBTest = {};



// This is used to add or remove EJSON from the beginning of everything nested
// inside an EJSON custom type. It should only be called on pure JSON!
var replaceNames = function (filter, thing) {
  if (typeof thing === "object") {
    if (_.isArray(thing)) {
      return _.map(thing, _.bind(replaceNames, null, filter));
    }
    var ret = {};
    _.each(thing, function (value, key) {
      ret[filter(key)] = replaceNames(filter, value);
    });
    return ret;
  }
  return thing;
};



var makeCouchDbLegal = function (name) { return "EJSON" + name; };
var unmakeCouchDbLegal = function (name) { return name.substr(5); };

// EJSON support needs to be looked into
var replaceCouchDBAtomWithMeteor = function (document) {
   
  
  if (document["EJSON$type"] && document["EJSON$value"]
      && _.size(document) === 2) {
    return EJSON.fromJSONValue(replaceNames(unmakeCouchDbLegal, document));
  }
  
 
  return undefined;
};

var replaceMeteorAtomWithCouchDB = function (document) {
 
  if (EJSON._isCustomType(document)) {
    return replaceNames(makeCouchDbLegal, EJSON.toJSONValue(document));
  }
  return undefined;
};

var replaceTypes = function (document, atomTransformer) {
  if (typeof document !== 'object' || document === null)
    return document;

  var replacedTopLevelAtom = atomTransformer(document);
  if (replacedTopLevelAtom !== undefined)
    return replacedTopLevelAtom;

  var ret = document;
  _.each(document, function (val, key) {
    var valReplaced = replaceTypes(val, atomTransformer);
    if (val !== valReplaced) {
      // Lazy clone. Shallow copy.
      if (ret === document)
        ret = _.clone(document);
      ret[key] = valReplaced;
    }
  });
  return ret;
};


CouchDBConnection = function (url, options) {
  var self = this;
  options = options || {};
  self._observeMultiplexers = {};
  self._onFailoverHook = new Hook;

  var couchDBOptions = {}; //{db: {safe: true}, server: {}, replSet: {}};
  self.Dbs = {};
    

  self.cloudant = null;
  // We keep track of the ReplSet's primary, so that we can trigger hooks when
  // it changes.  The Node driver's joined callback seems to fire way too
  // often, which is why we need to track it ourselves.
  self._primary = null;

  var connectFuture = new Future;
  Cloudant(
    url,
    Meteor.bindEnvironment(
      function (err, cloudant) {
        if (err) {
          throw err;
        }
        // Allow the constructor to return.
        connectFuture['return'](cloudant);
      },
      connectFuture.resolver()  // onException
    )
  );

  // Wait for the connection to be successful; throws on failure.
  self.cloudant = connectFuture.wait();
  
  if (options.global === true ) {
    //in couch, we need a separate changesListener per database.
    // so moved this into the driver.open() method
    //self._changesfeedHandle = new ChangesFeedHandle( url,  process.env.COUCHDB_DBNAME); 
    self._docFetcher = new DocFetcher(self);
  }
};

/*
CouchDBConnection.prototype.close = function() {
  var self = this;

  if (! self.db)
    throw Error("close called before Connection created?");

  // XXX probably untested
  // mario need to stop each DB changes observer.
  var oplogHandle = self.__changesfeedHandle;
  self._changesfeedHandle = null;
  if (oplogHandle)
    oplogHandle.stop();

  // Use Future.wrap so that errors get thrown. This happens to
  // work even outside a fiber since the 'close' method is not
  // actually asynchronous.
  Future.wrap(_.bind(self.db.close, self.db))(true).wait();
};*/

// Returns the CouchDB Database object; may yield. <Praveen, need to verify>
CouchDBConnection.prototype._getCollection = function (collectionName) {
  var self = this;

  if (! self.cloudant)
    throw Error("_getDB called before Connection created?");

  if (collectionName in self.Dbs) {
     return self.Dbs[collectionName];
  }
 
   
  self.Dbs[collectionName] = self.cloudant.use(collectionName);
  return self.Dbs[collectionName];
};


// This should be called synchronously with a write, to create a
// transaction on the current write fence, if any. After we can read
// the write, and after observers have been notified (or at least,
// after the observer notifiers have added themselves to the write
// fence), you should call 'committed()' on the object returned.
CouchDBConnection.prototype._maybeBeginWrite = function () {
  var self = this;
  var fence = DDPServer._CurrentWriteFence.get();
  if (fence)
    return fence.beginWrite();
  else
    return {committed: function () {}};
};

// Internal interface: adds a callback which is called when the server primary
// changes. Returns a stop handle.
// mario - not applicable to couchDB, so a no-op
CouchDBConnection.prototype._onFailover = function (callback) {
  return this._onFailoverHook.register(callback);
};


//////////// Public API //////////

// The write methods block until the database has confirmed the write (it may
// not be replicated or stable on disk, but one server has confirmed it) if no
// callback is provided. If a callback is provided, then they call the callback
// when the write is confirmed. They return nothing on success, and raise an
// exception on failure.
//
// After making a write (with insert, update, remove), observers are
// notified asynchronously. If you want to receive a callback once all
// of the observer notifications have landed for your write, do the
// writes inside a write fence (set DDPServer._CurrentWriteFence to a new
// _WriteFence, and then set a callback on the write fence.)
//
// Since our execution environment is single-threaded, this is
// well-defined -- a write "has been made" if it's returned, and an
// observer "has been notified" if its callback has returned.



var writeCallback = function (write, refresh, doc, callback, isUpdHandler) {
  return function (err, result, header) {
    if (! err) {
      // XXX We don't have to run this on error, right?
      var ts = new Date().getTime();
      if (typeof isUpdHandler != 'undefined' )	{
        var refreshDoc =  {id: doc._id, rev : header['x-couch-update-newrev'], _clientTs: ts};
      	refresh(refreshDoc);
      }
      else {
        var temp = _.clone(result);
        temp._clientTs = ts;
    	  refresh(temp); //mario change for waitUntilCaughtup
      }
    }
    write.committed();
    if (callback) {
       var retDoc
       if (typeof isUpdHandler != 'undefined' ) {
         callback(err, result); // update returns the update count.
       }else { 
    	 //retDoc =  _.extend({_rev: result.rev}, doc); // add the _rev
    	 if(err)
    	   callback(err, result);
    	 else
    	   callback(err, result.id); // mario match original mongo return.
       }
       
     }
    else if (err)
      throw err;
  };
};

var bindEnvironmentForWrite = function (callback) {
  return Meteor.bindEnvironment(callback, "couchdb write");
};

CouchDBConnection.prototype._insert = function (collection_name, document,
                                              callback) {
  var self = this;

  var sendError = function (e) {
    if (callback)
      return callback(e);
    throw e;
  };

  if (collection_name === "couchdb_meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    sendError(e);
    return;
  }

  if (!(LocalCollection._isPlainObject(document) &&
        !EJSON._isCustomType(document))) {
    sendError(new Error(
      "Only plain objects may be inserted into CouchDB"));
    return;
  }

  var write = self._maybeBeginWrite();
  var refresh = function (doc) { // mario waituntilCaughtup impl for couch
    //Meteor.refresh({collection: collection_name, id: document._id });
    var refreshKey = {collection: collection_name};
    Meteor.refresh(_.extend({_id: doc.id, _rev: doc.rev, _clientTs: doc._clientTs}, refreshKey));
    
  };
  var doc = replaceTypes(document, replaceMeteorAtomWithCouchDB);
  callback = bindEnvironmentForWrite(writeCallback(write, refresh, doc, callback));
  try {
    var collection = self._getCollection(collection_name);
     collection.insert(doc,
                      {safe: true}, callback);
  } catch (e) {
    write.committed();
    throw e;
  }
};

// Cause queries that may be affected by the selector to poll in this write
// fence.
CouchDBConnection.prototype._refresh = function (collectionName, selector, doc) {
  var self = this;
  var refreshKey = {collection: collectionName};
  // If we know which documents we're removing, don't poll queries that are
  // specific to other documents. (Note that multiple notifications here should
  // not cause multiple polls, since all our listener is doing is enqueueing a
  // poll.)
  var specificIds = LocalCollection._idsMatchedBySelector(selector);
  if (specificIds) {
    _.each(specificIds, function (id) {
      //Meteor.refresh(_.extend({id: id}, refreshKey));
      Meteor.refresh(_.extend({_id: id, _rev: doc.rev, _clientTs: doc._clientTs}, refreshKey));
    });
  } else {
    //Meteor.refresh(refreshKey);
    Meteor.refresh(_.extend({_id: doc.id, _rev: doc.rev, _clientTs: doc._clientTs}, refreshKey));
  }
};

CouchDBConnection.prototype._remove = function (collection_name, selector,
                                              callback) {
  var self = this;

  if (collection_name === "couchdb_meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    if (callback)
      return callback(e);
    else
      throw e;
  }

  var write = self._maybeBeginWrite();
  var doc = replaceTypes(selector, replaceMeteorAtomWithCouchDB);
  var refresh = function (doc) { // mario waituntilCaughtup impl for couch
    //self._refresh(collection_name, selector);
    self._refresh(collection_name, selector, doc);
  };
  callback = bindEnvironmentForWrite(writeCallback(write, refresh, doc, callback,true));

  try {
    var collection = self._getCollection(collection_name);
    //collection.destroy(doc._id,
    self.cloudant.request(
        { db: collection_name,
          path: '_design/meteor/_update/rem/' + doc._id,
          method: 'PUT',
          content_type: 'Content-Type:application/json',
          body: { _id : doc._id }
        }
        ,
        callback);
  } catch (e) {
    write.committed();
    throw e;
  }
};


CouchDBConnection.prototype._update = function (collection_name, selector, 
                                              options, callback) {
  var self = this;

  if (! callback && options instanceof Function) {
    callback = options;
    options = null;
  }

  if (collection_name === "couchdb_meteor_failure_test_collection") {
    var e = new Error("Failure test");
    e.expected = true;
    if (callback)
      return callback(e);
    else
      throw e;
  }

  
  if (!options) options = {};

  var write = self._maybeBeginWrite();
  var refresh = function (doc) { // mario waituntilCaughtup impl for couch
    //  self._refresh(collection_name, selector);
    self._refresh(collection_name, selector, doc);
  };
  var couchDBSelector = replaceTypes(selector, replaceMeteorAtomWithCouchDB);
  callback = writeCallback(write, refresh,couchDBSelector, callback, true);
  try {
    var collection = self._getCollection(collection_name);
    var knownId = selector._id;

    
    if (options.upsert && (! knownId) && options.insertedId) {
      couchDBSelector._id = options.insertedId;
      //     For more context, see
      //     https://github.com/meteor/meteor/issues/2278#issuecomment-64252706
      self.cloudant.request(
              { db: collection_name,
                path: '_design/meteor/_update/upsert/' + couchDBSelector._id,
                method: 'PUT',
                content_type: 'Content-Type:application/json',
                body: couchDBSelector
              },
              bindEnvironmentForWrite(function (err, result, header) {
                if (result && !options._returnObject) {
                  callback(err, result.numberAffected, header);
                }
                else {
                  callback(err, {numberAffected: result.numberAffected, insertedId: options.insertedId},header);
                }
              }));
    } else {
      var updHandlerFunc = '_design/meteor/_update/lastwritewins/';
      if (options.upsert) {
        updHandlerFunc = '_design/meteor/_update/upsert/';
      }
      self.cloudant.request(
        { db: collection_name,
          path:  updHandlerFunc + couchDBSelector._id,
          method: 'PUT',
          content_type: 'Content-Type:application/json',
          body: couchDBSelector
        },
        bindEnvironmentForWrite(function (err, result, header) {
          if (! err) {
            if (result && options._returnObject) {
              // If this was an upsert() call, and we ended up
              // inserting a new doc and we know its id, then
              // return that id as well.
              if (options.upsert && knownId 
                  &&  !result.updatedExisting ) { 
                result = { numberAffected: result.numberAffected , insertedId: knownId };
              }
              else {
                if (options.upsert)
                  result = { numberAffected: result.numberAffected };
                else
                  result = result.numberAffected;
              }
            }
            else if (options.upsert) { // return just the updatecount as integer.. upsert returns it an object
              result = result.numberAffected;
            }
          }
          callback(err, result,header);
        }));
    }
  } catch (e) {
    write.committed();
    throw e;
  }
};

var isModificationMod = function (mod) {
  var isReplace = false;
  var isModify = false;
  for (var k in mod) {
    if (k.substr(0, 1) === '$') {
      isModify = true;
    } else {
      isReplace = true;
    }
  }
  if (isModify && isReplace) {
    throw new Error(
      "Update parameter cannot have both modifier and non-modifier fields.");
  }
  return isModify;
};

var NUM_OPTIMISTIC_TRIES = 3;

// exposed for testing
CouchDBConnection._isCannotChangeIdError = function (err) {
  if (err.code === 13596)
    return true;
  if (err.err.indexOf("cannot change _id of a document") === 0)
    return true;

  // We don't use the error code
  // here, because the error code we observed it producing (16837) appears to be
  // a far more generic error code based on examining the source.
  if (err.err.indexOf("The _id field cannot be changed") === 0)
    return true;

  return false;
};


_.each(["insert", "update", "remove", "dropCollection"], function (method) {
  CouchDBConnection.prototype[method] = function () {
    var self = this;
    return Meteor.wrapAsync(self["_" + method]).apply(self, arguments);
  };
});


// XXX CouchDBConnection.upsert() does not return the id of the inserted document
// unless you set it explicitly in the selector or modifier (as a replacement
// doc).
CouchDBConnection.prototype.upsert = function (collectionName, selector, 
                                             options, callback) {
  var self = this;
  if (typeof options === "function" && ! callback) {
    callback = options;
    options = {};
  }

  return self.update(collectionName, selector, 
                     _.extend({}, options, {
                       upsert: true,
                       _returnObject: true
                     }), callback);
};


CouchDBConnection.prototype.find = function (collectionName, changesFeedHandle, selector, options) {
  var self = this;

  if (arguments.length === 1)
    selector = {};

  return new Cursor(
    self, changesFeedHandle, new CursorDescription(collectionName, selector, options));
};


CouchDBConnection.prototype.findOne = function (collection_name, changesFeedHandle, selector,
                                              options) {
  var self = this;
  if (arguments.length === 1)
    selector = {};
  
  // design docs should not come in the way for empty selector
  if ( !(_.isEmpty(selector) === true) ) {
    options = options || {};
    options.limit = 1;
  }
  return self.find(collection_name, changesFeedHandle, selector, options).fetch()[0];
};

// We'll actually design an index API later. For now, we just pass through to
// CouchDB's, but make it synchronous.
CouchDBConnection.prototype._ensureIndex = function (collectionName, indexdef,
                                                   options) {
  var self = this;
  options = _.extend({safe: true}, options);

  // We expect this function to be called at startup, not from within a method,
  // so we don't interact with the write fence.
  var db = self._getCollection(collectionName);
  var future = new Future;
  var idx;
  if (indexdef.constructor === Array )
    idx = {index: {fields: indexdef} };
  else {
    //mario - to allow text indexes etc, allow full indexDef through
    if ( _.has(indexdef,'index') )
      idx = indexdef;
    else
      idx = {index: {fields: [indexdef]} };
  }
  db.index(idx, future.resolver());
  future.wait();
};

CouchDBConnection.prototype._dropIndex = function (collectionName, index) {
  var self = this;

  // This function is only used by test code, not within a method, so we don't
  // interact with the write fence.
  var collection = self._getCollection(collectionName);
 //  var future = new Future;
 //  var indexName = collection.dropIndex(index, future.resolver());
 // future.wait();
 //mario no op for now. we dont return the index name in create, so how is user going to get ddoc
 
};

// CURSORS

// There are several classes which relate to cursors:
//
// CursorDescription represents the arguments used to construct a cursor:
// collectionName, selector, and (find) options.  Because it is used as a key
// for cursor de-dup, everything in it should either be JSON-stringifiable or
// not affect observeChanges output (eg, options.transform functions are not
// stringifiable but do not affect observeChanges).
//
// SynchronousCursor is a wrapper around a CouchDB cursor
// which includes fully-synchronous versions of forEach, etc.
//
// Cursor is the cursor object returned from find(), which implements the
// documented CouchDB.Database cursor API.  It wraps a CursorDescription and a
// SynchronousCursor.
//
// ObserveHandle is the "observe handle" returned from observeChanges. It has a
// reference to an ObserveMultiplexer.
//
// ObserveMultiplexer allows multiple identical ObserveHandles to be driven by a
// single observe driver.
//
// There are two "observe drivers" which drive ObserveMultiplexers:
//   - PollingObserveDriver caches the results of a query and reruns it when
//     necessary.
//   - OplogObserveDriver follows the operation log to directly observe
//     database changes.
// Both implementations follow the same simple interface: when you create them,
// they start sending observeChanges callbacks (and a ready() invocation) to
// their ObserveMultiplexer, and you stop them by calling their stop() method.

CursorDescription = function (collectionName, selector, options) {
  var self = this;
  self.collectionName = collectionName;
  self.selector = CouchDB.Database._rewriteSelector(selector);
  self.options = options || {};
};

Cursor = function (couchdbconn, changesFeedHandle, cursorDescription) {
  var self = this;

  self._couchdbconn = couchdbconn;
  self._cursorDescription = cursorDescription;
  self._synchronousCursor = null;
  self._changesFeedHandle = changesFeedHandle;
};

_.each(['forEach', 'map', 'fetch', 'count'], function (method) {
  Cursor.prototype[method] = function () {
    var self = this;

    // You can only observe a tailable cursor.
    if (self._cursorDescription.options.tailable)
      throw new Error("Cannot call " + method + " on a tailable cursor");

    if (!self._synchronousCursor) {
	
      self._synchronousCursor = self._couchdbconn._createSynchronousCursor(
        self._cursorDescription, {
          // Make sure that the "self" argument to forEach/map callbacks is the
          // Cursor, not the SynchronousCursor.
          selfForIteration: self,
          useTransform: true
        });
    }

    return self._synchronousCursor[method].apply(
      self._synchronousCursor, arguments);
  };
});

// Since we don't actually have a "nextObject" interface, there's really no
// reason to have a "rewind" interface.  All it did was make multiple calls
// to fetch/map/forEach return nothing the second time.
// XXX COMPAT WITH 0.8.1
Cursor.prototype.rewind = function () {
};

Cursor.prototype.getTransform = function () {
  return this._cursorDescription.options.transform;
};

// When you call Meteor.publish() with a function that returns a Cursor, we need
// to transmute it into the equivalent subscription.  This is the function that
// does that.

Cursor.prototype._publishCursor = function (sub) {
  var self = this;
  var collection = self._cursorDescription.collectionName;
  return CouchDB.Database._publishCursor(self, sub, collection);
};

// Used to guarantee that publish functions return at most one cursor per
// collection. Private, because we might later have cursors that include
// documents from multiple collections somehow.
Cursor.prototype._getCollectionName = function () {
  var self = this;
  return self._cursorDescription.collectionName;
}

Cursor.prototype.observe = function (callbacks) {
   var self = this;
  return LocalCollection._observeFromObserveChanges(self, callbacks); 
};

Cursor.prototype.observeChanges = function (callbacks) {
  var self = this;
  var ordered = LocalCollection._observeChangesCallbacksAreOrdered(callbacks); 
  return self._couchdbconn._observeChanges(self._cursorDescription, self._changesFeedHandle, ordered, callbacks);
};

  
CouchDBConnection.prototype._createSynchronousCursor = function(
    cursorDescription, options) {
  var self = this;
  options = _.pick(options || {}, 'selfForIteration', 'useTransform');

  
  var collection = self._getCollection(cursorDescription.collectionName);
  var cursorOptions = cursorDescription.options;
  
  var dbCursor;
  return new SynchronousCursor(collection, cursorDescription, options);
};


var SynchronousCursor = function (dbCursor, cursorDescription, options) {
  var self = this;
  options = _.pick(options || {}, 'selfForIteration', 'useTransform');

  if ( _.isEmpty(cursorDescription.selector) === true ) {
    cursorDescription.selector = {_id: { $gt:null } } ; // mario hack for all_docs with cloudant query
  }
  
  self._dbCursor = dbCursor;
  self._cursorDescription = cursorDescription;
  // The "self" argument passed to forEach/map callbacks. If we're wrapped
  // inside a user-visible Cursor, we want to provide the outer cursor!
  self._selfForIteration = options.selfForIteration || self;
  if (options.useTransform && cursorDescription.options.transform) {
    self._transform = LocalCollection.wrapTransform(
      cursorDescription.options.transform);
  } else {
    self._transform = null;
  }

  var options = cursorDescription.options;
  
  self._findQuery = {};
  self._findQuery.selector = cursorDescription.selector;
  
  if( _.isEmpty(options.sort) === false) {
    if(options.sort instanceof Array) {
    	self._findQuery.sort = options.sort;
    } else {
    	self._findQuery.sort = [options.sort];
    }
  }
  
  if( options.limit && options.limit > 0) {
	self._findQuery.limit = options.limit;
  }
  
  if( options.skip && options.skip > 0) {
	self._findQuery.skip = options.skip;
  }

  if( _.isEmpty(options.fields) === false) {
    if(options.fields instanceof Array) {
      self._findQuery.fields = options.fields;
    } else {
      self._findQuery.fields = [options.fields];
    }
    if ( _.indexOf(self._findQuery.fields,'_id') === -1 ) {
      self._findQuery.fields.push('_id');
    }
  }
  
  
  
  // Need to specify that the callback is the first argument to nextObject,
  // since otherwise when we try to call it with no args the driver will
  // interpret "undefined" first arg as an options hash and crash.
  /*self._synchronousNextObject = Future.wrap(
    /*dbCursor.nextObject.bind(dbCursor)*/ /*{text:true});*/
  self._synchronousNextObject = null;
  initCursor(this);
};

var initCursor = function(self) {
  
  if (  self._fetched === false )
	return;
  self._selectFuture = new Future;
  self._dbCursor.find(
    self._findQuery,
    Meteor.bindEnvironment(
      function (err, result) {
        if (err) {
          throw err;
        }
        // Allow the constructor to return.
        self._selectFuture['return'](result);
      },
      self._selectFuture.resolver()  // onException
    )
  );
  
  self._synchronousCount = 0;//Future.wrap(/*dbCursor.count.bind(dbCursor)*/ 1);
  self._visitedIds = new LocalCollection._IdMap;
  self._ctr = 0;
  self._fetched = false;
  self._result = null;
};

var fetchData = function(self) {
	try {
		self._result = self._selectFuture.wait();
		self._result.docs.forEach(function(doc) {
			if (doc._id.indexOf('_design/') != 0 ) // skip design docs 
				self._synchronousCount++;
		});
		
	} catch(e) {
		self._synchronousCount = 0;
		doc = null;
		Meteor._debug(e);
    }
	self._fetched = true;
};

_.extend(SynchronousCursor.prototype, {
  _nextObject: function () {
    var self = this;

    while (true) {
      var doc = null;
	  if( self._fetched === false) {
		fetchData(this);
	  }
  
	  if(!self._result || (self._result && self._ctr == self._result.docs.length)) {
		doc = null;
	  } else {
		doc = self._result.docs[self._ctr];
		self._ctr = self._ctr + 1;
	  }
	  
      

      if (!doc) { return null; }
	  if (doc._id.indexOf('_design/') === 0 ) // skip design docs
        continue;
      doc = replaceTypes(doc, replaceCouchDBAtomWithMeteor);

      if (!self._cursorDescription.options.tailable && _.has(doc, '_id')) {
        // Did CouchDB give us duplicate documents in the same cursor? If so,
        // ignore this one. (Do this before the transform, since transform might
        // return some unrelated value.) We don't do this for tailable cursors,
        // because we want to maintain O(1) memory usage. And if there isn't _id
        // for some reason (maybe it's the oplog), then we don't do this either.
        // (Be careful to do this for falsey but existing _id, though.)
        if (self._visitedIds.has(doc._id)) continue;
        self._visitedIds.set(doc._id, true);
      }

      if (self._transform)
        doc = self._transform(doc);

      return doc;
    }
  },

  forEach: function (callback, thisArg) {
    var self = this;

    // Get back to the beginning.
    self._rewind();

    // We implement the loop ourself instead of using self._dbCursor.each,
    // because "each" will call its callback outside of a fiber which makes it
    // much more complex to make this function synchronous.
    var index = 0;
    while (true) {
      var doc = self._nextObject();
      if (!doc) return;
      callback.call(thisArg, doc, index++, self._selfForIteration);
    }
  },

  // XXX Allow overlapping callback executions if callback yields.
  map: function (callback, thisArg) {
    var self = this;
    var res = [];
    self.forEach(function (doc, index) {
      res.push(callback.call(thisArg, doc, index, self._selfForIteration));
    });
    return res;
  },

  _rewind: function () {
	//Re-initialize the cursor
	initCursor(this);
  },

  // Mostly usable for tailable cursors.
  close: function () {
    var self = this;

    //self._dbCursor.close();
  },

  fetch: function () {
    var self = this;
    if( self._fetched === false) {
	   fetchData(self);
	 }
    return self.map(_.identity);
  },

  count: function () {
    var self = this;
    if( self._fetched === false) {
		fetchData(self);
	 }
	 return self._synchronousCount;
	  
  },

  // This method is NOT wrapped in Cursor.
  getRawObjects: function (ordered) {
    var self = this;
    if (ordered) {
      return self.fetch();
    } else {
      var results = new LocalCollection._IdMap;
      self.forEach(function (doc) {
        results.set(doc._id, doc);
      });
      return results;
    }
  }
});

CouchDBConnection.prototype.tail = function (dbname,cursorDescription, docCallback) {
  
  var self = this;
  if (!cursorDescription.options.tailable)
    throw new Error("Can only tail a tailable cursor");

  self.initDB(dbname);
  
  var feed = self._getCollection(dbname).follow({since: "now",include_docs:true});
  var changeFuture = new Future;
  feed.on('change',function(change) {
    changeFuture['return'](change);
  });
  feed.follow();
       
   
  var loop = function () { 
      while (true) {
        if (stopped)
          return;
        var change = changeFuture.wait();
        if (stopped)
          return;
          
        if (change.id.indexOf('_design/') === 0 ) {// skip design docs
          changeFuture = new Future;
          continue;
        }
        
        
        if(change.hasOwnProperty('deleted') && change.deleted === true ) {
          docCallback({ ts: new Date(), op:'d', o:  change.doc   });
        }
        else {
            docCallback({ ts: new Date(), op:'u', o: change.doc , o2: {_id: change.id} });
        }
        changeFuture = new Future;
        
      }
    
    };
    
  Meteor.defer(loop);

  return {
    stop: function () {
      stopped = true;
      //cursor.close(); todo close the follow feed
    }
  };
};


CouchDBConnection.prototype._observeChanges = function (
    cursorDescription, changesFeedHandle, ordered, callbacks) {
  
  var self = this;

  if (cursorDescription.options.tailable) {
    return self._observeChangesTailable(cursorDescription, ordered, callbacks);
  }

  // You may not filter out _id when observing changes, because the id is a core
  // part of the observeChanges API.
  if (cursorDescription.options.fields &&
      (cursorDescription.options.fields._id === 0 ||
       cursorDescription.options.fields._id === false)) {
    throw Error("You may not observe a cursor with {fields: {_id: 0}}");
  }

  var observeKey = JSON.stringify(
    _.extend({ordered: ordered}, cursorDescription));

  var multiplexer, observeDriver;
  var firstHandle = false;

  // Find a matching ObserveMultiplexer, or create a new one. This next block is
  // guaranteed to not yield (and it doesn't call anything that can observe a
  // new query), so no other calls to this function can interleave with it.
  Meteor._noYieldsAllowed(function () {
    if (_.has(self._observeMultiplexers, observeKey)) {
      multiplexer = self._observeMultiplexers[observeKey];
    } else {
      firstHandle = true;
      // Create a new ObserveMultiplexer.
      multiplexer = new ObserveMultiplexer({
        ordered: ordered,
        onStop: function () {
          delete self._observeMultiplexers[observeKey];
          observeDriver.stop();
        }
      });
      self._observeMultiplexers[observeKey] = multiplexer;
    }
  });

  var observeHandle = new ObserveHandle(multiplexer, callbacks);

  if (firstHandle) {
    var matcher, sorter;
    var canUseOplog = _.all([
      function () {
        // At a bare minimum, using the oplog requires us to have an oplog, to
        // want unordered callbacks, and to not want a callback on the polls
        // that won't happen.
        return !ordered &&
          !callbacks._testOnlyPollCallback;
      }, function () {
        // We need to be able to compile the selector. Fall back to polling for
        // some newfangled $selector that minimongo doesn't support yet.
        try {
          matcher = new Minimongo.Matcher(cursorDescription.selector);
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      }, function () {
        // ... and the selector itself needs to support oplog.
       
        return ChangesObserveDriver.cursorSupported(cursorDescription, matcher);
      }, function () {
        // And we need to be able to compile the sort, if any.  eg, can't be
        // {$natural: 1}.
        if (!cursorDescription.options.sort)
          return true;
        try {
          sorter = new Minimongo.Sorter(cursorDescription.options.sort,
                                        { matcher: matcher });
          return true;
        } catch (e) {
          // XXX make all compilation errors MinimongoError or something
          //     so that this doesn't ignore unrelated exceptions
          return false;
        }
      }], function (f) { return f(); });  // invoke each function

    var driverClass = canUseOplog ? ChangesObserveDriver : PollingObserveDriver;
    observeDriver = new driverClass({
      cursorDescription: cursorDescription,
      couchdbConn: self,
      changesFeedHandle : changesFeedHandle,
      multiplexer: multiplexer,
      ordered: ordered,
      matcher: matcher,  // ignored by polling
      sorter: sorter,  // ignored by polling
      _testOnlyPollCallback: callbacks._testOnlyPollCallback
    });

    // This field is only set for use in tests.
    multiplexer._observeDriver = observeDriver;
  }

  // Blocks until the initial adds have been sent.
  multiplexer.addHandleAndSendInitialAdds(observeHandle);

  return observeHandle;
};

// Listen for the invalidation messages that will trigger us to poll the
// database for changes. If this selector specifies specific IDs, specify them
// here, so that updates to different specific IDs don't cause us to poll.
// listenCallback is the same kind of (notification, complete) callback passed
// to InvalidationCrossbar.listen.

listenAll = function (cursorDescription, listenCallback) {
  var listeners = [];
  forEachTrigger(cursorDescription, function (trigger) {
    listeners.push(DDPServer._InvalidationCrossbar.listen(
      trigger, listenCallback));
  });

  return {
    stop: function () {
      _.each(listeners, function (listener) {
        listener.stop();
      });
    }
  };
};

forEachTrigger = function (cursorDescription, triggerCallback) {
  var key = {collection: cursorDescription.collectionName};
  var specificIds = LocalCollection._idsMatchedBySelector(
    cursorDescription.selector);
  if (specificIds) {
    _.each(specificIds, function (id) {
      triggerCallback(_.extend({id: id}, key));
    });
    triggerCallback(_.extend({dropCollection: true, id: null}, key));
  } else {
    triggerCallback(key);
  }
};

// observeChanges for tailable cursors on capped collections.
// not useable for couch.
//
CouchDBConnection.prototype._observeChangesTailable = function (
    cursorDescription, ordered, callbacks) {
  var self = this;

  // Tailable cursors only ever call added/addedBefore callbacks, so it's an
  // error if you didn't provide them.
  if ((ordered && !callbacks.addedBefore) ||
      (!ordered && !callbacks.added)) {
    throw new Error("Can't observe an " + (ordered ? "ordered" : "unordered")
                    + " tailable cursor without a "
                    + (ordered ? "addedBefore" : "added") + " callback");
  }

  return self.tail(cursorDescription, function (doc) {
    var id = doc._id;
    delete doc._id;
    // The ts is an implementation detail. Hide it.
    delete doc.ts;
    if (ordered) {
      callbacks.addedBefore(id, doc, null);
    } else {
      callbacks.added(id, doc);
    }
  });
};


CouchDBConnection.prototype.initDB = function (dbname) {
  var self = this;

  var f = new Future;
  var createDbcallback = function(err,body) {
    if (err) {
      throw err;
    }
    f['return'](false); // console.log('db created. need to create updatehandler');
  };
 
  var updatehandlerCallback = function(err,body) {
    if (err) {
      if (err.statusCode === undefined || err.statusCode === 404) { // not found
        self.cloudant.db.create(dbname, createDbcallback); // db not exists, create it
      }
      else if (err.statusCode === 409) { 
        f['return'](true); //console.log('everything already setup');
      }
      else {
        throw err;
      }
    }
    else {
      f['return'](true); //console.log('updateHandler created');
    }
  };
  
  while (true) {
    self._getCollection(dbname).insert(
     { updates: { lastwritewins : function(doc, req) {if (doc) { var userdoc = JSON.parse(req.body); userdoc._rev = doc._rev; return [userdoc, '1']; } else { return [null,'0'];} }, upsert : function(doc, req) {if (doc) { var userdoc = JSON.parse(req.body); userdoc._rev = doc._rev; return [userdoc, toJSON({numberAffected:1 , updatedExisting: true})]; } else { var userdoc = JSON.parse(req.body); return [userdoc,toJSON({numberAffected:1 , updatedExisting: false})];} }, rem : function(doc, req) { if (doc) { var userdoc = JSON.parse(req.body); userdoc._rev = doc._rev; userdoc._deleted = true; return [userdoc, '1']; } else { return [null,'0'];} } }}
    ,
    '_design/meteor',
    updatehandlerCallback);
     
    var ret = f.wait(); 
    if ( ret === false ) {
      f = new Future;
      continue;
    }
    else
      break;
  }
  
}
CouchDBInternals.Connection = CouchDBConnection;
CouchDBInternals.NpmModule = Cloudant;
