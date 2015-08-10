Package.describe({
  name: 'cloudant:couchdb',
  version: '0.0.1',
  // Brief, one-line summary of the package.
  summary: 'Full stack database driver for CouchDB/Cloudant in Meteor',
  // URL to the Git repository containing the source code for this package.
  git: 'https://github.com/cloudant/meteor-couchdb',
  // By default, Meteor will default to using README.md for documentation.
  // To avoid submitting documentation, set this field to null.
  documentation: 'README.md'
});

Npm.depends({
  cloudant: "1.2.0",
  "double-ended-queue": "2.1.0-0",
  "collections": "1.2.2"
   
});

Package.onUse(function(api) {
api.use(['random', 'ejson', 'json', 'underscore',  'minimongo', 'logging', 
           'ddp', 'tracker'],
          ['client', 'server']);
          
  api.use('check', ['client', 'server']);
  
  // Binary Heap data structure is used to optimize oplog observe driver
  // performance.
  api.use('binary-heap', 'server'); 
  
   // Allow us to detect 'insecure'.
  api.use('insecure', {weak: true});

  // Allow us to detect 'autopublish', and publish collections if it's loaded.
  api.use('autopublish', 'server', {weak: true});

   // defaultRemoteCollectionDriver gets its deployConfig from something that is
  // (for questionable reasons) initialized by the webapp package.
  api.use('webapp', 'server', {weak: true});

  // If the facts package is loaded, publish some statistics.
  api.use('facts', 'server', {weak: true});

  api.use('callback-hook', 'server');

  // Stuff that should be exposed via a real API, but we haven't yet.
  api.export('CouchDBInternals', 'server');
  // For tests only.
  api.export('CouchDB');
  api.export('CouchDBTest');

  api.addFiles(['couchdb_driver.js','changes_tailing.js',
                'observe_multiplex.js','changes_observe_driver.js',
                'polling_observe_driver.js','doc_fetcher.js'],
                'server');

  api.addFiles('local_collection_driver.js', ['client', 'server']);
  api.addFiles('remote_collection_driver.js', 'server');
  api.addFiles('collection.js', ['client', 'server']);
  api.versionsFrom('1.1.0.2');
  
});

Package.onTest(function(api) {
  api.use('tinytest');
  api.use('cloudant:couchdb');
  api.use(['tinytest', 'underscore', 'test-helpers', 'ejson', 'random',
           'ddp', 'base64']);
  api.addFiles('allow_tests.js', ['client', 'server']);
  api.addFiles('doc_fetcher_tests.js', 'server');
  api.addFiles('collection_tests.js', ['client', 'server']);
  api.addFiles('oplog_tests.js', 'server');
  api.addFiles('observe_changes_tests.js', ['client', 'server']);
  api.addFiles('couchdb_livedata_tests.js', ['client', 'server']);
  api.addFiles('callbackloggercouch.js', ['client', 'server']);
});
