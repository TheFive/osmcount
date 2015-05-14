
var path    = require('path');
var fs      = require('fs');
var debug   = require('debug')('configuration');
var env = process.env.NODE_ENV || 'development';
var pg = require('pg');





// the configurationfile should be in the "running" directory
var configurationFile = path.resolve(__dirname, 'config.'+env+'.json');
var configuration;


var configurationInitialised = false;




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







exports.getServerPort = function() {
	return configuration.serverport;
}

exports.getDatabaseType = function() {
  return configuration.databaseType;
}
