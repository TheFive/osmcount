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
    if (typeof(mongodb)=='undefined') console.log("MongoDB Undefined");
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
            