
var path    = require('path');
var fs      = require('fs');
var debug   = require('debug')('configuration');
  debug.entry = require('debug')('configuration:entry');
  debug.data = require('debug')('configuration:data');
  
var env = process.env.NODE_ENV || 'development';

var MongoClient = require('mongodb').MongoClient;




// the configurationfile should be in the "running" directory
var configurationFile = path.resolve(__dirname, 'config.'+env+'.json');
var configuration;

var mongodb ;


getDBString = function() {
  debug.entry('getDBString');
  configuration=exports.getConfiguration();
  var userString = "";
  if (configuration.username != "") {
    userString = configuration.username + ':'
                 + configuration.password +'@';
  }
  var mongodbConnectStr ='mongodb://'
             + userString 
             + configuration.database;
  return mongodbConnectStr;        
}

var initialisedDB = 0;
var initialiseCB = [];

exports.initialiseDB = function(callback) {
  debug.entry('initialiseDB');
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
	var mongodbConnectStr = getDBString();
	debug.data("Connect to mongo db with string: %",mongodbConnectStr);
	MongoClient.connect(mongodbConnectStr, function(err, db) {
		debug.entry("initialiseDB->CB");
		var returnerr;
		if (err) {
			console.log("Failed to connect to MongoDB");
			console.log(err);
			initialiseDB = 0;
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

exports.initialise = function(callback) {
	try {
	   configuration = JSON.parse(fs.readFileSync(configurationFile));
	} catch (err) {
	 configuration = JSON.parse(fs.readFileSync(travisConfigurationFile));
	}
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
	return mongodb;
}


exports.getServerPort = function() {
	return configuration.serverport;
}
            