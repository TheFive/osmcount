var async   = require('async');
var path    = require('path');
var program = require('commander');
var debug   = require('debug')('export');
var mkdirp  = require('mkdirp');

// require own modules
var config         = require('./configuration.js');
var util           = require('./util.js');

// require model modules
var DataCollection = require('./model/DataCollection.js');
var WorkerQueue    = require('./model/WorkerQueue.js');
var DataTarget     = require('./model/WorkerQueue.js');


program
  .option('-f [filename]','filename [filename]','')
  .option('-e --export','Export Data')
  .option('-i --import','import Data')
  .option('--datacollection','Use DataCollection Container')
  .option('--workerqueue', 'Use WorkerQueue Container')
  .option('--datatarget', 'Use DatTarget Container')
  .option('--mongo','Use MongoDB')
  .option('--postgres','Use PostgresDB')
  .option('--all')
  .parse(process.argv);

var dirname = path.join(__dirname);


if (program.F=='') {
  console.log("Please enter a Filename");
  process.exit();
} else {
	debug('setting filenames');
  if (program.all) {
  	dirname = path.join(__dirname,program.F);
  	program.dcFilename=path.join(dirname,"datacollection.json");  
  	program.dtFilename=path.join(dirname,"datatarget.json")  ;
  	program.wqFilename=path.join(dirname,"workerqueue.json") ; 
  	
  } else {
	  program.dcFilename=path.join(dirname,program.F);
	  program.dtFilename=path.join(dirname,program.F);  
	  program.wqFilename=path.join(dirname,program.F); 
  }
}

debug('initialising util');
util.initialise();

function importData(cb) {
	debug('importData');
  if (!program.import) {
    cb();
    return;
  };
  if (program.mongo) {
  	cb("Import MongoDB not supported yet");
  	return;
  }
  DataCollection.initialise("postgres");
  DataTarget.initialise("postgres");
  WorkerQueue.initialise("postgres");
  debug('posgres initialised');
  async.series([
  	function(callback) {
		  if (program.datacollection || program.all) {
        console.log("Start Import DataCollection (PostgresDB)");
			  DataCollection.import(program.dcFilename,callback);
		  } else callback(); 		
  	},
  	function(callback) {
		  if (program.datatarget || program.all) {
        console.log("Start Import DataTarget (PostgresDB)");
			  DataTarget.import(program.dtFilename,callback);
		  } else callback(); 
  	},
  	function(callback) {
		  if (program.workerqueue || program.all) {
        console.log("Start Import WorkerQueue (PostgresDB)");
			  WorkerQueue.import(program.wqFilename,callback);
		  } else callback(); 
  	}
  	],function(err) {cb(err)});
}

function createTables(callback) {
	if(!program.all || !program.import ) {
		callback();
		return;
	} 
	async.series([
		DataCollection.dropTable,
		DataCollection.createTable,
		DataTarget.dropTable,
		DataTarget.createTable,
		WorkerQueue.dropTable,
		WorkerQueue.createTable
		],function(err) {callback(err);});
}

function exportData(cb) {
	debug('exportData');
  if (!program.export) {
  	debug('nothing to do');
    cb();
    return;
  };
  if (program.postgres) {
  	cb("Import PosgresDB not supported yet");
  	return;
  }
  DataCollection.initialise("mongo");
  DataTarget.initialise("mongo");
  WorkerQueue.initialise("mongo");
  debug('mongo initialised');
  async.series([
  	function(callback) {
  		mkdirp(dirname,callback);
  	},
   	function(callback) {
		  if (program.datacollection || program.all) {
        console.log("Start Export DataCollection (MongoDB)");
			  DataCollection.export(program.dcFilename,callback);
		  } else callback();  		
  	},
  	function(callback) {
		  if (program.datatarget || program.all) {
        console.log("Start Export DataTarget (MongoDB)");
			  DataTarget.export(program.dtFilename,callback);
		  } else callback(); 
  	},
  	function(callback) {
		  if (program.workerqueue || program.all) {
        console.log("Start Export WorkerQueue (MongoDB)");
			  WorkerQueue.export(program.wqFilename,callback);
		  } else callback(); 
  	}
  	],function(err) {cb(err)});
}


function handleImportExport(cb) {
  async.auto(
    {
    createTables: createTables,
    importData:   ["createTables",importData],
    exportData:   exportData
    }, function(err) {cb(err);})
}


console.log("Start Data Export / Import");

async.auto( {
    config: config.initialise,
    mongodb: ["config",config.initialiseMongoDB],
    postgresdb: ["config",config.initialisePostgresDB],
    handleImportExport: ["mongodb","postgresdb",handleImportExport]
  },
  function (err) {
    if (err) {
      console.log("Error occured: "+err);
      console.dir(err);
    }
    else {
      console.log("export / import done");
    }
    process.exit();
  }
)
