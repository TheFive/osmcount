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
var DataTarget     = require('./model/DataTarget.js');
var OSMData        = require('./model/OSMData.js');


program
  .option('-f [filename]','filename [filename]','')
  .option('-e --export','Export Data')
  .option('-i --import','import Data')
  .option('--datacollection','Use DataCollection Container')
  .option('--workerqueue', 'Use WorkerQueue Container')
  .option('--datatarget', 'Use DataTarget Container')
  .option('--osmdata', 'Use OSMData Container')
  .option('--mongo','Use MongoDB')
  .option('--postgres','Use PostgresDB')
  .option('--all')
  .parse(process.argv);

var dirname = path.join(__dirname);

if (!program.export && ! program.import) {
  console.log("You better tell me, what is should do with -e or -i. Try --help");
  process.exit();
}

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
    program.odFilename=path.join(dirname,"osmdata.json") ; 
  	
  } else {
	  program.dcFilename=path.join(dirname,program.F);
	  program.dtFilename=path.join(dirname,program.F);  
    program.wqFilename=path.join(dirname,program.F); 
    program.odFilename=path.join(dirname,program.F); 
  }
}

if ((program.mongo!= true) && !(program.postrges != true)) {
  console.log("Please Enter a Database");
  console.log(program.mongo);
  console.log(program.postgres);
  process.exit();
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
  OSMData.initialise("postgres");
  debug('postgres initialised');
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
    },
    function(callback) {
      if (program.osmdata || program.all) {
        console.log("Start Import OSMData (PostgresDB)");
        OSMData.import(program.odFilename,callback);
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
		DataCollection.dropTable.bind(DataCollection),
		DataCollection.createTable.bind(DataCollection),
		DataTarget.dropTable.bind(DataTarget),
		DataTarget.createTable.bind(DataTarget),
		WorkerQueue.dropTable.bind(WorkerQueue),
		WorkerQueue.createTable.bind(WorkerQueue),
    OSMData.dropTable.bind(OSMData),
    OSMData.createTable.bind(OSMData)
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
  OSMData.initialise('mongo');
  debug('mongo initialised');
  async.series([
  	function(callback) {
  		mkdirp(dirname,callback);
  	},
   	function(callback) {
		  if (program.datacollection || program.all) {
			  DataCollection.export(program.dcFilename,callback);
		  } else callback();  		
  	},
  	function(callback) {
		  if (program.datatarget || program.all) {
			  DataTarget.export(program.dtFilename,callback);
		  } else callback(); 
  	},
    function(callback) {
      if (program.workerqueue || program.all) {
        WorkerQueue.export(program.wqFilename,callback);
      } else callback(); 
    },
    function(callback) {
      if (program.osmdata || program.all) {
        OSMData.export(program.odFilename,callback);
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
    handleImportExport: ["mongodb",handleImportExport]
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
