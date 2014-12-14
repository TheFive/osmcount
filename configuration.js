var fs, configurationFile;
var path=require('path');
var fs = require('fs');




// the configurationfile should be in the "running" directory
configurationFile = path.resolve(__dirname, 'configuration.json');

var configuration;

var MongoClient = require('mongodb').MongoClient;
var mongodb ;


exports.getDBString = function() {
	configuration=exports.getConfiguration();
	var mongodbConnectStr ='mongodb://'
                   + configuration.username + ':'
                   + configuration.password + '@'
                   + configuration.database;
	return mongodbConnectStr;        
}

initialisedDB = false;
exports.initialiseDB = function(callback) {
    configuration=exports.getConfiguration();
	if (!initialisedDB && typeof(mongodb == 'undefined')) {
		initialisedDB=true;
		var mongodbConnectStr = exports.getDBString();
		console.log("connect:"+mongodbConnectStr);
		MongoClient.connect(mongodbConnectStr, function(err, db) {
			if (err) throw err;
			mongodb = db;
			console.log("Connected to Database mosmcount");
			if (callback) callback();
		})
	} 

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

exports.getDB = function() {
	return mongodb;
}


exports.getServerPort = function() {
	return configuration.serverport;
}
            