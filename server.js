//require node modules
var express = require('express');
var async   = require('async');
var path    = require('path');
var debug   = require('debug')('server');
 debug.entry   = require('debug')('server:entry');
 debug.data    = require('debug')('server:data');

// require own modules
var config           = require('./configuration.js');
var queue            = require('./QueueWorker.js');
var loadDataFromDB   = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js')
var display          = require('./display.js');
var util             = require('./util.js');
var plotlyexport        = require('./plotlyexport.js');

var importDataCollection = require('./controller/importDataCollection.js');
var displayAggregationTable = require('./controller/displayAggregationTable.js')

var app = express();


exports.dirname = __dirname;

util.initialise();

// Initialise all Modules in the right sequence

debug("Start Async Configuration");
async.auto( {
		config: config.initialise,
		mongodb: ["config",config.initialiseMongoDB],
    postgresdb: ["config",config.initialisePostgresDB],
		dbdata:  ["mongodb", loadDataFromDB.initialise],
		startQueue: ["dbdata",queue.startQueue]


		//	,insertJobs: ["startQueue",queue.insertJobs]
	},
	function (err) {
		if (err) throw(err);

		debug("Async Configuration Ready");
	}
)


process.on( 'SIGINT', function() {
  console.log( "\nRequest for Shutdown OSMCount, Please wait for OverpassQuery. SIGINT (Ctrl-C)" );
  queue.processSignal = 'SIGINT';
})



debug("Initialising HTML Routes");
// log every call and then call more detailled
// and publish Mongo DB to all functions via res
app.use(function(req, res, next){
    debug("Url Called: %s",req.url);
    res.db= config.getMongoDB();
    next();
});

app.use('/count.html', display.count);
app.use('/index.html', display.main);
app.use('/waplot/:measure.html', plotlyexport.plot);
app.use('/wavplot/:measure.html', plotlyexport.plotValues);
app.use('/import/csvimport.html', importDataCollection.showPage);
app.use('/import/:measure.html', display.importApotheken);
app.use('/table/:measure.:type', displayAggregationTable.table);
app.use('/object/:collection/:id.html', display.object);
app.use('/overpass/:measure/:schluessel.html', display.overpass);
app.use('/wa/:aufgabe.html',display.wochenaufgabe);
app.use('/list/:query.html',display.query);
app.use('/', express.static(path.resolve(__dirname, "html")));


app.get('/*', function(req, res) {
    res.status(404).sendFile(path.resolve(__dirname, "html",  'error.html'));
});



debug("Start Listening to Server Port");
// Start to lisen on port

app.listen(config.getServerPort());


console.log("Server has started and is listening to localhost:"+config.getServerPort());
