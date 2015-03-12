var async   = require('async');
var path    = require('path');
var debug   = require('debug')('export');

// require own modules
var config           = require('./configuration.js');
var util             = require('./util.js');
var DataCollection = require('./model/DataCollection.js');



exports.dirname = __dirname;

util.initialise();

function importPostgresDB(cb) {
  DataCollection.initialise("postgres");
  DataCollection.import("datacollection.json",cb);
}
function exportMongoDB(cb) {
  DataCollection.initialise("mongo");
  DataCollection.export("datacollection.json",cb);

}

debug("Start Async Configuration");
async.auto( {
		config: config.initialise,
		mongodb: ["config",config.initialiseMongoDB],
    postgresdb: ["config",config.initialisePostgresDB],
    export: ["mongodb",exportMongoDB],
    import: ["export","postgresdb",importPostgresDB]
	},
	function (err) {
		if (err) throw(err);

		console.log("export / import done");
	}
)
