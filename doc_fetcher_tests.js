var Fiber = Npm.require('fibers');
var Future = Npm.require('fibers/future');

testAsyncMulti("couchdb-livedata - doc fetcher", [
  function (test, expect) {
    var self = this;
    var collName = "docfetcher-" + Random.id().toLowerCase();
    var collection = new CouchDB.Database(collName);
    var id1 = collection.insert({x: 1});
    var id2 = collection.insert({y: 2});

    var fetcher = new CouchDBTest.DocFetcher(
      CouchDBInternals.defaultRemoteCollectionDriver().couchdb);

    // Test basic operation.
    var expected1 = {_id: id1, x: 1};
    fetcher.fetch(collName, id1, Random.id(), CouchDBInternals.defaultRemoteCollectionDriver()._changesfeedHandle, 
        expect(function (e, d) {
          fetched = true;
          test.isFalse(e);
          delete d._rev;
          test.equal(d, expected1);
        }));
        
   
    fetcher.fetch(collName, "nonexistent!", Random.id(), CouchDBInternals.defaultRemoteCollectionDriver()._changesfeedHandle, expect(null, null));

    var fetched = false;
    var cacheKey = Random.id();
    var expected = {_id: id2, y: 2};
    fetcher.fetch(collName, id2, cacheKey, CouchDBInternals.defaultRemoteCollectionDriver()._changesfeedHandle,
        expect(function (e, d) {
      fetched = true;
      test.isFalse(e);
      delete d._rev;
      test.equal(d, expected);
    }));
    // The fetcher yields.
    test.isFalse(fetched);

    // Now ask for another document with the same cache key. Because a fetch for
    // that cache key is in flight, we will get the other fetch's document, not
    // this random document.
    fetcher.fetch(collName, Random.id(), cacheKey,CouchDBInternals.defaultRemoteCollectionDriver()._changesfeedHandle,
        expect(function (e, d) {
      test.isFalse(e);
      delete d._rev;
      test.equal(d, expected);
    }));
  }
]);
