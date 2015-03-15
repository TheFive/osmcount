var async   = require('async');
var path    = require('path');
var debug   = require('debug')('export');

// require own modules
var config         = require('./configuration.js');
var util           = require('./util.js');

// require model modules
var DataCollection = require('./model/DataCollection.js');
var WorkerQueue    = require('./model/WorkerQueue.js');



exports.dirname = __dirname;

util.initialise();

function importPostgresDB(cb) {
  console.log("Start Import Data Collection (PostgresDB)");
  DataCollection.initialise("postgres");
  DataCollection.import("datacollection.json",cb);
}
function exportMongoDBDataCollection(cb) {
  console.log("Start Export Data Collection (MongoDB)");
  DataCollection.initialise("mongo");
  DataCollection.export("datacollection.json",cb);

}
function exportMongoDBWorkerQueue(cb) {
  console.log("Start Export WorkerQueue (MongoDB)")
  DataCollection.initialise("mongo");
  WorkerQueue.export("WorkerQueue.json",cb);

}

console.log("Start Data Export");

async.auto( {
    config: config.initialise,
    mongodb: ["config",config.initialiseMongoDB],
    postgresdb: ["config",config.initialisePostgresDB],
    exportWorkerQueue: ["mongodb",exportMongoDBWorkerQueue],
    exportDataCollection: ["exportWorkerQueue",exportMongoDBDataCollection]
    //import: ["postgresdb",importPostgresDB]
  },
  function (err) {
    if (err) {
      console.log("Error occured: "+err);
    }
    else {
      console.log("export / import done");
      process.exit();
    }
  }
)
