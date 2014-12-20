//require node modules
var express = require('express');
var async   = require('async');
var debug   = require('debug')('server');
 debug.entry   = require('debug')('server:entry');
 debug.data    = require('debug')('QueueWorker:data');

// require own modules
var config           = require('./configuration.js');
var queue            = require('./QueueWorker.js');
var loadDataFromDB   = require('./LoadDataFromDB.js');
var loadOverpassData = require('./loadOverpassData.js')
var display          = require('./display.js');


var app = express();



// Initialise all Modules in the right sequence

debug("Start Async Configuration");
async.auto( {
		config: config.initialise,
		mongodb: ["config",config.initialiseDB],
		dbdata:  ["mongodb", loadDataFromDB.initialise],
		startQueue: ["dbdata",queue.startQueue]
		
		
		//	,insertJobs: ["startQueue",queue.insertJobs]
	}, 
	function (err) {
		if (err) throw(err);
		debug("Async Configuration Ready");
	}
)




debug("Initialising HTML Routes");
// log every call and then call more detailled
// and publish Mongo DB to all functions via res
app.use(function(req, res, next){
    debug("Url Called: %s",req.url);
    res.db= config.getDB();
    next();
});


app.use('/', express.static(__dirname));
app.use('/count.html', display.count)
app.use('/import.html', display.importCSV)
app.use('/table.html', display.table)


app.use('/list.html', function(req,res) {
	jobs = loadOverpassData.createQuery("AddrWOStreet");
	res.end(JSON.stringify(jobs))
})

app.get('/*', function(req, res) {
    res.status(404).sendFile(__dirname + '/error.html');
});



debug("Start Listening to Server Port");
// Start to lisen on port
app.listen(config.getServerPort());



console.log("Server has started and is listening to localhost:"+config.getServerPort());
	