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
