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

LocalCollectionDriver = function () {
  var self = this;
  self.noConnDbs = {};
};

var ensureCollection = function (name, dbs) {
  if (!(name in dbs)) {
     dbs[name] = new LocalCollectionWrapper(name);
  }
  return dbs[name];
};

_.extend(LocalCollectionDriver.prototype, {
  open: function (name, conn) {
    var self = this;
    if (!name) {
      return new LocalCollectionWrapper;
    }
    if (! conn) {
        return ensureCollection(name, self.noConnDbs);
    }
    if (! conn._couchdb_livedata_collections)
      conn._couchdb_livedata_collections = {};
      // XXX is there a way to keep track of a connection's collections without
    // dangling it off the connection object?
    return ensureCollection(name, conn._couchdb_livedata_collections);
  }
});

// singleton
LocalCollectionDriver = new LocalCollectionDriver;

// wrap for
// - syntactic sugar variations in cloudant query and minimongo query -> asc/desc vs 1/-1 in sort syntax,
// - translate a cloudant update(doc) to  minimongo.update({_id : doc._id} , doc)
// - change asc/desc in sort to 1/-1 and not an array, just object

LocalCollectionWrapper = function(name) {
  var self = this;
  if (name === undefined)
    self._lc = new LocalCollection;
  else
    self._lc = new LocalCollection(name);
};

var convertToObj = function(value, key ,list) {
  for (first in value) {
    if (value[first] === 'asc')
      this[first] = 1 ;
    else
      this[first] = -1 ;
	break;
  }
}

var convertToObjFields = function(value, key ,list) {
  this[value] = 1;
}

_.each(["find","findOne","insert","update","remove","allow","deny",
        "pauseObservers","resumeObservers","saveOriginals","retrieveOriginals"
        ], function (name) {
  LocalCollectionWrapper.prototype[name] = function (/* arguments */) {
    
    var self = this;
    var args = _.toArray(arguments);
    
    if ( name ==='update' ) {
      // make the selector just the _id for update.
      var doc =  _.clone(args[0]) || {};
      args[0] = {_id : doc._id};
      // make the modifier the replace document
      if (args.length === 3 ) {
        args[3] = _.clone(args[2]);
        args[2] = _.clone(args[1]);
      }
      else if (args.length === 2 ) {
        args[2] = _.clone(args[1]);
      }
      args[1] = doc;
    }
    else if(name ==='findOne' || name ==='find'  ) {
      // sort is inside options which is 2nd arg
       var options =  _.clone(args[1]) || {};
       if (options.sort !== undefined && options.sort instanceof Array) {
         var jsonVariable = {};
         _.each(options.sort, convertToObj,jsonVariable);
         options.sort = jsonVariable;
         args[1] = options;
       } 
       
       // now fields
       var options =  _.clone(args[1]) || {};
       if (options.fields !== undefined && options.fields instanceof Array) {
         var jsonVariable = {};
         _.each(options.fields, convertToObjFields,jsonVariable);
         options.fields = jsonVariable;
         args[1] = options;
       } 
      
    }
    return self._lc[name].apply(self._lc, args);
  };	
  
});


