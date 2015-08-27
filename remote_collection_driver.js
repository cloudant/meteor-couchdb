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

CouchDBInternals.RemoteCollectionDriver = function (
  couchdb_url, options) {
  var self = this;
  this._url = couchdb_url;
  self.couchdb = new CouchDBConnection(couchdb_url, options);
};

_.extend(CouchDBInternals.RemoteCollectionDriver.prototype, {
  open: function (name) {
    var self = this;
    self._changesfeedHandle = new ChangesFeedHandle(this._url, name); 
    var ret = {};
    _.each(
      ['find', 'findOne', 'insert', 'update', 'remove', '_ensureIndex', '_dropIndex',
       'upsert'],
//       
//       , '_createCappedCollection',
 //      'dropCollection'],
      function (m) {
        if (m==='find' || m==='findOne') {
          ret[m] = _.bind(self.couchdb[m], self.couchdb, name, self._changesfeedHandle);
        } else {
          ret[m] = _.bind(self.couchdb[m], self.couchdb, name);
        }
      });
    return ret;
  }
});


// Create the singleton RemoteCollectionDriver only on demand, so we
// only require CouchDB configuration if it's actually used (eg, not if
// you're only trying to receive data from a remote DDP server.)
CouchDBInternals.defaultRemoteCollectionDriver = _.once(function () {
  
  var connectionOptions = {};

  var couchdbUrl = process.env.COUCHDB_URL;
  connectionOptions.global = true;

  if (! couchdbUrl)
    throw new Error("COUCHDB_URL must be set in environment");

  return new CouchDBInternals.RemoteCollectionDriver(couchdbUrl, connectionOptions);
});
