
var path    = require('path');
var fs      = require('fs');
var debug   = require('debug')('configuration');
var MongoClient = require('mongodb').MongoClient;




// the configurationfile should be in the "running" directory
var configurationFile = path.resolve(__dirname, 'configuration.json');

var configuration;

var mongodb ;


getDBString = function() {
	configuration=exports.getConfiguration();
	var mongodbConnectStr ='mongodb://'
                   + configuration.username + ':'
                   + configuration.password + '@'
                   + configuration.database;
	return mongodbConnectStr;        
}

var initialisedDB = 0;
var initialiseCB = [];
exports.initialiseDB = function(callback) {

   
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
	debug("Connect to mongo db with string: %",mongodbConnectStr);
	MongoClient.connect(mongodbConnectStr, function(err, db) {
		if (err) throw err;
		initialisedDB=2;
		mongodb = db;
		debug("Connected to Database mosmcount");
		while (initialiseCB.length>0) {
			initialiseCB[0]();
			initialiseCB.shift();
		}
	})
}

exports.initialise = function(callback) {
	configuration = JSON.parse(fs.readFileSync(configurationFile));
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
            