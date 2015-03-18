var async   = require('async');
var path    = require('path');
var program = require('commander');
var debug   = require('debug')('export');

// require own modules
var config         = require('./configuration.js');
var util           = require('./util.js');

// require model modules
var DataCollection = require('./model/DataCollection.js');
var WorkerQueue    = require('./model/WorkerQueue.js');


program
  .option('-f [filename]','filename [filename]','')
  .option('-e --export','Export Data')
  .option('-i --import','import Data')
  .option('--datacollection','Use DataCollection Container')
  .option('--workerqueue', 'Use WorkerQueue Container')
  .option('--mongo','Use MongoDB')
  .option('--postgres','Use PostgresDB')
  .parse(process.argv);

exports.dirname = __dirname;


if (program.F=='') {
  console.log("Please enter a Filename");
  process.exit();
} else {
  program.filename = program.F;
}

util.initialise();

function importPostgresDBDataCollection(cb) {
  if (!program.import || !program.postgres || !program.datacollection) {
    cb();
    return;
  };
  console.log("Start Import Data Collection (PostgresDB)");
  DataCollection.initialise("postgres");
  DataCollection.import(program.filename,cb);
}
function importPostgresDBWorkerQueue(cb) {
  if (!program.import || !program.postgres || !program.workerqueue) {
    cb();
    return;
  };
  console.log("Start Import WorkerQueue (PostgresDB)");
  WorkerQueue.initialise("postgres");
  WorkerQueue.import(program.filename,cb);
}

function importMongoDBDataCollection(cb) {
  if (!program.import || !program.mongo || !program.datacollection) {
    cb();
    return;
  };
  console.log("Import DataCollection Mongo DB not implemented yet.");
  cb();
}
function importMongoDBWorkerQueue(cb) {
  if (!program.import || !program.mongo || !program.workerqueue) {
    cb();
    return;
  };
  console.log("Import WorkerQueue Mongo DB not implemented yet.");
  cb();
}

function exportMongoDBDataCollection(cb) {
  if (!program.export || !program.mongo || !program.datacollection) {
    cb();
    return;
  };
  console.log("Start Export Data Collection (MongoDB)");
  DataCollection.initialise("mongo");
  DataCollection.export(program.filename,cb);

}
function exportMongoDBWorkerQueue(cb) {
  if (!program.export || !program.mongo || !program.workerqueue) {
    cb();
    return;
  };
  console.log("Start Export WorkerQueue (MongoDB)")
  DataCollection.initialise("mongo");
  WorkerQueue.export(program.filename,cb);
}
function exportPostgresDBDataCollection(cb) {
  if (!program.export || !program.postgres || !program.datacollection) {
    cb();
    return;
  };
  console.log("Export Data Collection (PostgresDB) not implemented yet");
  cb();
}
function exportPostgresDBWorkerQueue(cb) {
  if (!program.export || !program.postgres || !program.workerqueue) {
    cb();
    return;
  };
  console.log("Export WorkerQueue (PostgresDB) not implemented yet");
  cb();
}

function handleImportExport(cb) {
  async.parallel(
    [
    importMongoDBDataCollection,
    importMongoDBWorkerQueue,
    exportMongoDBDataCollection,
    exportMongoDBWorkerQueue,
    importPostgresDBWorkerQueue,
    importPostgresDBDataCollection,
    exportPostgresDBWorkerQueue,
    exportPostgresDBDataCollection
    ], function(err) {cb(err);})
}


console.log("Start Data Export");

async.auto( {
    config: config.initialise,
    mongodb: ["config",config.initialiseMongoDB],
    postgresdb: ["config",config.initialisePostgresDB],
    handleImportExport: ["mongodb","postgresdb",handleImportExport]
  },
  function (err) {
    if (err) {
      console.log("Error occured: "+err);
    }
    else {
      console.log("export / import done");
    }
    process.exit();
  }
)
