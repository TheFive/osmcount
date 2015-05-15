
var path    = require('path');
var fs      = require('fs');
var debug   = require('debug')('configuration');
var env = process.env.NODE_ENV || 'development';
var pg = require('pg');

var MongoClient = require('mongodb').MongoClient;




// the configurationfile should be in the "running" directory
var configurationFile = path.resolve(__dirname, 'config.'+env+'.json');
var configuration;

var mongodb ;

var configurationInitialised = false;


function getMongoDBString() {
  debug('getMongoDBString');
  configuration=exports.getConfiguration();
  var userString = "";
  if (configuration.mongodb.username != "") {
    userString = configuration.mongodb.username + ':'
                 + configuration.mongodb.password +'@';
  }
  var mongodbConnectStr ='mongodb://'
             + userString
             + configuration.mongodb.database;
  return mongodbConnectStr;
}

function getPostgresDBString() {
  debug('getPostgresDBString');
  configuration=exports.getConfiguration();
  var userString = "";
  if (configuration.postgres.username != "") {
    userString = configuration.postgres.username + ':'
                 + configuration.postgres.password +'@';
  }
  var connectStr ='postgres://'
             + userString
             + configuration.postgres.database;
  if ((typeof(configuration.postgres.connectstr)!='undefined' ) &&
      (configuration.postgres.connectstr != '' )) {
        connectStr = configuration.postgres.connectstr;
      }
  return connectStr;
}

var initialisedDB = 0;
var initialiseCB = [];

exports.initialiseMongoDB = function(callback) {
  debug('initialiseMongoDB');
  if (!configurationInitialised) exports.initialise();

  if (configuration.mongodb.initialise == false) {
    callback();
    return;
  }
  // Implement a run one behaviour
  if (initialisedDB == 2) {
    // nothing to do
    if (callback) callback();
    return;
  }
  if (initialisedDB ==1) {
    // Initialising in Progress, extend Callback List
    if (callback) initialiseCB.push(callback);
    return;
  }
  if (callback) initialiseCB.push(callback);
  configuration=exports.getConfiguration();
	initialisedDB=1;
	var mongodbConnectStr = getMongoDBString();
	debug("Connect to mongo db with string: %",mongodbConnectStr);
	MongoClient.connect(mongodbConnectStr, function(err, db) {
		debug("initialiseMongoDB->CB");
		var returnerr;
		if (err) {
			console.log("Failed to connect to MongoDB");
			console.log("Connect String: "+mongodbConnectStr);
			console.log(err);
			initialisedDB = 0;
			returnerr = err;
		} else {
			initialisedDB=2;
			mongodb = db;
		}
		debug("Connected to Database "+mongodbConnectStr);
		while (initialiseCB.length>0) {
			initialiseCB[0](returnerr);
			initialiseCB.shift();
		}
	})
}


exports.postgresConnectStr;

exports.initialisePostgresDB = function(callback) {
  debug('initialisePostgresDB');
  console.log("configuration.initialisePostgresDB This Function is unnecessary and will be skipped")
  if (!configurationInitialised) exports.initialise();

  // Minimal Behaviour just store One Connect String
  // Connection is established by pg object in retrieaving
  // a client
  if (configuration.postgres.initialise == false) {
    callback();
    return;
  }

  configuration=exports.getConfiguration();
  pg.defaults.poolSize = 25;
	exports.postgresConnectStr = getPostgresDBString();
  if (callback) callback();
}





exports.initialise = function(callback) {
  debug("initialise");
  if (configurationInitialised) {
    if (callback) callback();
    return;
  }
  configurationInitialised = true;
	console.log("Reading Config from: "+configurationFile);
	configuration = JSON.parse(fs.readFileSync(configurationFile));
   pg.defaults.poolSize = 25;
  exports.postgresConnectStr = getPostgresDBString();
	if (callback) callback();
}




exports.getConfiguration = function() {
    if (typeof(configuration)=='undefined')
    {
    	exports.initialise();
    }
	return configuration;
}
exports.getValue = function(key,defValue) {
    if (typeof(configuration)=='undefined')
    {
    	exports.initialise();
    }
    var result = defValue;
    if (typeof(configuration[key]) != 'undefined') {
    	result = configuration[key];
    }
    return result;
}


exports.getDB = function() {
  console.log('configuration.getDB deprecated, use configuration.getMongoDB instead.')
	return mongodb;
}

exports.getMongoDB = function() {
  return mongodb;
}


exports.getServerPort = function() {
	return configuration.serverport;
}

exports.getDatabaseType = function() {
  return configuration.databaseType;
}
