Tinytest.add(
  'collection - call Mongo.Collection without new',
  function (test) {
    test.throws(
      function () {
        CouchDB.Database(null);
      },
      /use "new" to construct a CouchDB\.Database/
    );
  }
);
