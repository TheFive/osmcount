//require node modules
var express = require('express');
var async   = require('async');
var path    = require('path');
var debug   = require('debug')('server');
 debug.entry   = require('debug')('server:entry');
 debug.data    = require('debug')('QueueWorker:data');

// require own modules
var config           = require('./configuration.js');
var queue            = require('./QueueWorker.js');
var loadDataFromDB   = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js')
var display          = require('./display.js');
var util             = require('./util.js');
var plotlyexport        = require('./plotlyexport.js');


var app = express();

util.initialise();

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

app.use('/count.html', display.count);
app.use('/index.html', display.main);
app.use('/waplot/:measure.html', plotlyexport.plot);
app.use('/import/csvimport.html', display.importCSV);
app.use('/import/apotheken.html', display.importApotheken);
app.use('/table.html', display.table);
app.use('/table/:measure.:type', display.table);
app.use('/object/:collection/:id', display.object);
app.use('/overpass/:measure/:schluessel.html', display.overpass);
app.use('/wa/:aufgabe.html',display.wochenaufgabe);
app.use('/', express.static(path.resolve(__dirname, "html")));


app.get('/*', function(req, res) {
    res.status(404).sendFile(path.resolve(__dirname, "html",  'error.html'));
});



debug("Start Listening to Server Port");
// Start to lisen on port
app.listen(config.getServerPort());



console.log("Server has started and is listening to localhost:"+config.getServerPort());
	

