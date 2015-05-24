//require node modules
var express = require('express');
var async   = require('async');
var path    = require('path');
var debug   = require('debug')('server');

// require own modules
var config           = require('./configuration.js');
var queue            = require('./QueueWorker.js');
var loadDataFromDB   = require('./model/LoadDataFromDB.js');
var loadOverpassData = require('./model/LoadOverpassData.js')
var DataTarget       = require('./model/DataTarget.js')
var DataCollection   = require('./model/DataCollection.js')
var WorkerQueue      = require('./model/WorkerQueue.js')
var display          = require('./display.js');
var util             = require('./util.js');
var plotlyexport     = require('./plotlyexport.js');

var importDataCollection    = require('./controller/ImportDataCollection.js');
var displayAggregationTable = require('./controller/displayAggregationTable.js')
var displayText             = require('./controller/displayText.js')
var displayObject           = require('./controller/displayObject.js')
var displayStatus           = require('./controller/displayStatus.js')

var app = express();


exports.dirname = __dirname;

util.initialise();

// Initialise all Modules in the right sequence

debug("Start Async Configuration");
async.auto( {
		config: config.initialise,
        datatarget: ["config",DataTarget.initialise.bind(DataTarget)],
        workerqueue: ["config",WorkerQueue.initialise.bind(WorkerQueue)],
        datacollection: ["config",DataCollection.initialise.bind(DataCollection)],
		dbdata:  ["config", loadDataFromDB.initialise],
		startQueue: ["dbdata","workerqueue","datacollection","datatarget",queue.startQueue]


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
  console.log("Overpass Running:"+queue.overpassRunning);
  console.log("Overpass Running since:"+(new Date(queue.overpassStartTime)).toLocaleString());
  console.log('Overpass Measure:'+queue.overpassMeasure);
  console.log('Overpass Location:'+queue.overpassLocation);
  queue.processExit = function() {process.exit();}
})



debug("Initialising HTML Routes");
// log every call and then call more detailled
app.use(function(req, res, next){
    debug("Url Called: %s",req.url);
    next();
});

app.use('/index.html', displayText.main);
app.use('/status.html', displayStatus.status);
app.use('/waplot/:measure.html', plotlyexport.plot);
app.use('/import/csvimport.html', importDataCollection.showPage);
app.use('/import/:measure.html', display.importApotheken);
app.use('/table/:measure.:type', displayAggregationTable.table);
app.use('/object/WorkerQueue/IgnoreError', displayObject.ignoreError);
app.use('/object/:collection/:id.html', displayObject.object);
app.use('/overpass/:measure/:schluessel.html', displayAggregationTable.overpass);
app.use('/wa/:aufgabe.html',displayText.wochenaufgabe);
//app.use('/list/pharmacy.html',displayText.pharmaciesNotImplemented);
app.use('/list/:query.html',display.query);
app.use('/', express.static(path.resolve(__dirname, "html")));


app.get('/*', function(req, res) {
    res.status(404).sendFile(path.resolve(__dirname, "html",  'error.html'));
});



debug("Start Listening to Server Port");
// Start to lisen on port

app.listen(config.getServerPort());


console.log("Server has started and is listening to localhost:"+config.getServerPort());
