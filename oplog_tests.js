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

var couchprefix = "couchdb_";

var OplogCollection = new CouchDB.Database("oplog-" + Random.id().toLowerCase());

Tinytest.add("couchdb-livedata - oplog - cursorSupported", function (test) {
  var oplogEnabled =
        !!true;

  var supported = function (expected, selector, options) {
    var cursor = OplogCollection.find(selector, options);
    var handle = cursor.observeChanges({added: function () {}});
    // If there's no oplog at all, we shouldn't ever use it.
    if (!oplogEnabled)
      expected = false;
    test.equal(!!handle._multiplexer._observeDriver._usesOplog, expected);
    handle.stop();
  };

  supported(true, "asdf");
  supported(true, 1234);
  supported(true, new CouchDB.ObjectID());

  supported(true, {_id: "asdf"});
  supported(true, {_id: 1234});
  supported(true, {_id: new CouchDB.ObjectID()});

  supported(true, {foo: "asdf",
                   bar: 1234,
                   baz: new CouchDB.ObjectID(),
                   eeney: true,
                   miney: false,
                   moe: null});

  supported(true, {});

  supported(true, {$and: [{foo: "asdf"}, {bar: "baz"}]});
  supported(true, {foo: {x: 1}});
  supported(true, {foo: {$gt: 1}});
  supported(true, {foo: [1, 2, 3]});

  // No $where.
  supported(false, {$where: "xxx"});
  supported(false, {$and: [{foo: "adsf"}, {$where: "xxx"}]});
  // No geoqueries.
  supported(false, {x: {$near: [1,1]}});
  // Nothing Minimongo doesn't understand.  (Minimongo happens to fail to
  // implement $elemMatch inside $all which server supports.)
  supported(false, {x: {$all: [{$elemMatch: {y: 2}}]}});

  supported(true, {}, { sort: {x:1} });
  supported(true, {}, { sort: {x:1}, limit: 5 });
  supported(false, {}, { sort: {$natural:1}, limit: 5 });
  supported(false, {}, { limit: 5 });
  supported(false, {}, { skip: 2, limit: 5 });
  supported(false, {}, { skip: 2 });
});

testAsyncMulti(
  "couchDB-livedata - oplog - entry skipping", [
    function (test, expect) {
      var self = this;
      self.collectionName = Random.id();
      self.collection = new CouchDB.Database(couchprefix + self.collectionName.toLowerCase());
      self.collection._ensureIndex({species: 'asc'});

      // Fill collection with lots of irrelevant objects (red cats) and some
      // relevant ones (blue dogs).
      self.IRRELEVANT_SIZE = 5;
      self.RELEVANT_SIZE = 10;
      var docs = [];
      var i;
      for (i = 0; i < self.IRRELEVANT_SIZE; ++i) {
        var doc = {
            name: "cat " + i,
            species: 'cat',
            color: 'red'
          };
        docs.push(doc);
        self.collection.insert(doc, Meteor.bindEnvironment(expect(function (err) {
          test.isFalse(err);
        })));
      }
      for (i = 0; i < self.RELEVANT_SIZE; ++i) {
        var doc =  {
            name: "dog " + i,
            species: 'dog',
            color: 'blue'
          };
        docs.push(doc);
        self.collection.insert(doc, Meteor.bindEnvironment(expect(function (err) {
          test.isFalse(err);
        })));
      }
      // XXX implement bulk insert #1255
     
     
    },
    function (test, expect) {
      var self = this;

      test.equal(self.collection.find().count(),
                 self.IRRELEVANT_SIZE + self.RELEVANT_SIZE);

      var blueDog5Id = null;
      var gotSpot = false;

      // Watch for blue dogs.
      self.subHandle =
        self.collection.find({species: 'dog', color: 'blue'}).observeChanges({
          added: function (id, fields) {
            if (fields.name === 'dog 5')
              blueDog5Id = id;
          },
          changed: function (id, fields) {
            if (EJSON.equals(id, blueDog5Id) && fields.name === 'spot')
              gotSpot = true;
          }
        });
      test.isTrue(self.subHandle._multiplexer._observeDriver._usesOplog);
      test.isTrue(blueDog5Id);
      test.isFalse(gotSpot);

      self.skipped = false;
      self.skipHandle =
        CouchDBInternals.defaultRemoteCollectionDriver()
        ._changesfeedHandle.onSkippedEntries(function () {
          self.skipped = true;
        });

      // Dye all the cats blue. This adds lots of oplog mentries that look like
      // they might in theory be relevant (since they say "something you didn't
      // know about is now blue", and who knows, maybe it's a dog) which puts
      // the OplogObserveDriver into FETCHING mode, which performs poorly.
      self.collection.find({species: 'cat'}).forEach(function (doc){
        doc.color = 'blue';
        self.collection.update(doc);
      });
      
      var bd5 = self.collection.findOne(blueDog5Id);
      bd5.name = 'spot';
      self.collection.update(bd5);

      // We ought to see the spot change soon!  It's important to keep this
      // timeout relatively small (ie, small enough that if we set
      // $METEOR_OPLOG_TOO_FAR_BEHIND to something enormous, say 200000, that
      // the test fails).
      pollUntil(expect, function () {
        return gotSpot;
      }, 2000);
    },
    function (test, expect) {
      var self = this;
      //test.isTrue(self.skipped);

      self.skipHandle.stop();
      self.subHandle.stop();
      //self.collection.remove({});
    }
  ]
);
