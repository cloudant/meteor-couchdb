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

var Future = Npm.require('fibers/future');
var Deque = Npm.require('double-ended-queue');
var LruSet = Npm.require("collections/lru-set");
var FastMap = Npm.require("collections/fast-map");


var TOO_FAR_BEHIND = process.env.METEOR_CHANGESFEED_TOO_FAR_BEHIND || 2000;

// Like Perl's quotemeta: quotes all regexp metacharacters. See
//   https://github.com/substack/quotemeta/blob/master/index.js
// XXX this is duplicated with accounts_server.js
var quotemeta = function (str) {
    return String(str).replace(/(\W)/g, '\\$1');
};

var showTS = function (ts) {
  return "Timestamp(" + ts.getHighBits() + ", " + ts.getLowBits() + ")";
};

idForOp = function (op) {
  if (op.op === 'd')
    return op.o._id;
  else if (op.op === 'i')
    return op.o._id;
  else if (op.op === 'u')
    return op.o2._id;
  else if (op.op === 'c')
    throw Error("Operator 'c' doesn't supply an object with id: " +
                EJSON.stringify(op));
  else
    throw Error("Unknown op: " + EJSON.stringify(op));
};

ChangesFeedHandle = function ( url, dbName) {
  var self = this;
  
  self._dbName = dbName;
  self._url = url;

  self._oplogLastEntryConnection = null;
  self._oplogTailConnection = null;
  self._stopped = false;
  self._tailHandle = null;
  self._readyFuture = new Future();
  self._crossbar = new DDPServer._Crossbar({
    factPackage: "couchdb-livedata", factName: "changes-watchers"
  });
  
  self._catchingUpFutures = [];
  self._lastProcessedTS = null;
  self._waitingChanges = new FastMap();
  self._lastWaited = null;
  self._tooFastKeys = new LruSet(null,TOO_FAR_BEHIND);
  
  self._onSkippedEntriesHook = new Hook({
    debugPrintExceptions: "onSkippedEntries callback"
  });

  self._entryQueue = new Deque();
  self._workerActive = false;
  
  
  self._startTailing();
};

_.extend(ChangesFeedHandle.prototype, {
  stop: function () {
    var self = this;
    if (self._stopped)
      return;
    self._stopped = true;
    if (self._tailHandle)
      self._tailHandle.stop();
    // XXX should close connections too
  },
  onOplogEntry: function (trigger, callback) {
    
    var self = this;
    if (self._stopped)
      throw new Error("Called onOplogEntry on stopped handle!");

    // Calling onOplogEntry requires us to wait for the tailing to be ready.
    self._readyFuture.wait();
    

    var originalCallback = callback;
    callback = Meteor.bindEnvironment(function (notification) {
      // XXX can we avoid this clone by making oplog.js careful?
    
      originalCallback(EJSON.clone(notification));
    }, function (err) {
      Meteor._debug("Error in oplog callback", err.stack);
    });
    var listenHandle = self._crossbar.listen(trigger, callback);
    return {
      stop: function () {
        listenHandle.stop();
      }
    };
  },
  // Register a callback to be invoked any time we skip oplog entries (eg,
  // because we are too far behind).
  onSkippedEntries: function (callback) {
    var self = this;
    if (self._stopped)
      throw new Error("Called onSkippedEntries on stopped handle!");
    return self._onSkippedEntriesHook.register(callback);
  },
  // Calls `callback` once the oplog has been processed up to a point that is
  // roughly "now": specifically, once we've processed all ops that are
  // currently visible.
  // XXX become convinced that this is actually safe even if oplogConnection
  // is some kind of pool
  waitUntilCaughtUpFlush: function(notification) {
    var self = this;
    if (self._stopped)
      throw new Error("Called waitUntilCaughtUp on stopped handle!");

    // Calling waitUntilCaughtUp requries us to wait for the oplog connection to
    // be ready.
    self._readyFuture.wait();

    if (self._stopped)
      return;
    
    var ts = notification._clientTs;
    if (!ts)
      throw Error("WriteCallback without ts: " + EJSON.stringify(notification._clientTs));

    if (self._lastProcessedTS && ts <= self._lastProcessedTS) {
      // We've already caught up to here.
      return;
    }
    
    var key = notification._id + ' ' + notification._rev;
    if ( self._tooFastKeys.get(key) ) {
      return; // _changes went before us
    }
    
    
    self._waitingChanges.add(ts,key);
    // Insert the future into our list. Almost always, this will be at the end,
    // but it's conceivable that if we fail over from one primary to another,
    // the oplog entries we see will go backwards.
    var insertAfter = self._catchingUpFutures.length;
    while (insertAfter - 1 > 0
           && self._catchingUpFutures[insertAfter - 1].ts.greaterThan(ts)) {
      insertAfter--;
    }
    var f = new Future;
    self._catchingUpFutures.splice(insertAfter, 0, {ts: ts, future: f});
    self._lastWaited = ts;
    f.wait();
    
  },
  
  waitUntilCaughtUp: function () {
    var self = this;
    if (self._stopped)
      throw new Error("Called waitUntilCaughtUp on stopped handle!");

    // Calling waitUntilCaughtUp requries us to wait for the oplog connection to
    // be ready.
    self._readyFuture.wait();

    if (self._stopped)
      return;

    var lastEntry;
    if (!lastEntry) {
      // Really, nothing in the oplog? Well, we've processed everything.
      return;
    }

    
  },
  
  _startTailing: function () {
    var self = this;
    

    self._oplogTailConnection = new CouchDBConnection(
    self._url, {});
    
    
    var cursorDescription = new CursorDescription(
      ' ', {}, {tailable: true});
    
    self._tailHandle = self._oplogTailConnection.tail(
      self._dbName, cursorDescription, function (doc) {
        self._entryQueue.push(doc);
        self._maybeStartWorker();
      }
    );
   

    self._readyFuture.return();
  },

  _maybeStartWorker: function () {
    
    var self = this;
    
    if (self._workerActive)
      return;
    self._workerActive = true;
    Meteor.defer(function () {
      try {
        
        while (! self._stopped && ! self._entryQueue.isEmpty()) {
          // Are we too far behind? Just tell our observers that they need to
          // repoll, and drop our queue.
          if (self._entryQueue.length > TOO_FAR_BEHIND) {
            self._entryQueue.clear();

            self._onSkippedEntriesHook.each(function (callback) {
              callback();
              return true;
            });

            // Free any waitUntilCaughtUp() calls that were waiting for us to
            // pass something that we just skipped.
            if (self._lastWaited) {
              self._setLastProcessedTS(self._lastWaited);
              self._lastWaited = null;
            }
            self._waitingChanges.clear();
            continue;
          }

          var doc = self._entryQueue.shift();

          var trigger = {collection: self._dbName,
                         dropCollection: false,
                         op: doc};
          
          // Is it a special command and the collection name is hidden somewhere
          // in operator?
          if (trigger.collection === "$cmd") {
            
            trigger.collection = doc.o.drop;
            trigger.dropCollection = true;
            trigger.id = null;
          } else {
           
            // All other ops have an id.
            trigger.id = idForOp(doc); 
          }

          self._crossbar.fire(trigger);

          // Now that we've processed this operation, process pending
          // sequencers.
          var key = doc.o._id + ' ' + doc.o._rev;
          var ts = self._waitingChanges.get(key);
          if (!ts) {
            // direct db change ? or _changes received before meteor's on flush
            // this _tooFastKeys is for the latter.
            self._tooFastKeys.add(key); // do we need a timestamp too here ?
            return;
          }
          self._waitingChanges.delete(key);
          self._setLastProcessedTS(ts);
           
        }
      } finally {
        self._workerActive = false;
      }
    });
  },
  
  _setLastProcessedTS: function (ts) {
    var self = this;
    self._lastProcessedTS = ts;
    while (!_.isEmpty(self._catchingUpFutures)
           && self._catchingUpFutures[0].ts <= 
             self._lastProcessedTS) {
      var sequencer = self._catchingUpFutures.shift();
      sequencer.future.return();
    }
  },
  
  _clearWaits: function() {
    var self = this;
    self._processedChangesSet.clear();
    
    var clearFutures = function (value, key, collection ) {
      if(value)
        value.return();
      collection.delete(key);
    };
    
    self._waitingFutures.forEach(clearFutures);
        
  }
  
});
