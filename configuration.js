var fs, configurationFile;
var path=require('path');

configurationFile = path.resolve(__dirname, 'configuration.json');
fs = require('fs');

var configuration;

// Log the information from the file
console.log(configuration);



var MongoClient = require('mongodb').MongoClient;
var mongodb;








exports.initialise = function() {
	configuration = JSON.parse(fs.readFileSync(configurationFile));
}



  
exports.getConfiguration = function() {
	return configuration;
}

exports.getDB = function() {
	async.series ( [
		function (callback) {
			if (typeof(mongodb == 'undefined')) {
				var mongodbConnectStr = config.getDBString();
                console.log("connect:"+mongodbConnectStr);
				MongoClient.connect(mongodbConnectStr, function(err, db) {
					if (err) throw err;
					mongodb = db;
					console.log("Connected to Database mosmcount");
					callback();
				})
			} else {
				callback();
			}	
		}
		,
		function (callback) {
			if (typeof(mongodb == 'undefined')) console.log ("Mongodb undefined, why is series not working");
			callback();
		}])	
	return mongodb;
}

exports.getDBString = function() {
   var mongodbConnectStr ='mongodb://'
                   + configuration.username + ':'
                   + configuration.password + '@'
                   + configuration.database;
    console.log("connect:"+mongodbConnectStr);
    return mongodbConnectStr;        
}

exports.getServerPort = function() {
	return configuration.serverport;
}
            