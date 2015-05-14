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
  .option('--all')
  .option('--createTable','drop and create Table before import')
  .parse(process.argv);

var dirname = path.join(__dirname);

if (!program.export && ! program.import) {
  console.log("You better tell me, what I should do with -e or -i. Try --help");
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


debug('initialising util');
util.initialise();

function importData(cb) {
	debug('importData');
  if (!program.import) {
    cb();
    return;
  };
  DataCollection.initialise("postgres");
  DataTarget.initialise("postgres");
  WorkerQueue.initialise("postgres");
  OSMData.initialise("postgres");
  debug('postgres initialised');
  async.series([
    function(callback) {
      if (program.datacollection && program.createTable) {
        console.log("Create Table DataCollection (PostgresDB)");
        async.series([
          DataCollection.dropTable.bind(DataCollection),
          DataCollection.createTable.bind(DataCollection),
        ],function(err) {callback(err);});        
      } else callback();    
    },
    function(callback) {
      if (program.datatarget && program.createTable) {
        console.log("Create Table DataTarget (PostgresDB)");
        async.series([
          DataTarget.dropTable.bind(DataTarget),
          DataTarget.createTable.bind(DataTarget),
        ],function(err) {callback(err);});        
      } else callback();    
    },
    function(callback) {
      if (program.osmdata && program.createTable) {
        console.log("Create Table OSMData (PostgresDB)");
        async.series([
          OSMData.dropTable.bind(OSMData),
          OSMData.createTable.bind(OSMData),
        ],function(err) {callback(err);});        
      } else callback();    
    },
    function(callback) {
      if (program.workerqueue && program.createTable) {
        console.log("Create Table WorkerQueue (PostgresDB)");
        async.series([
          WorkerQueue.dropTable.bind(WorkerQueue),
          WorkerQueue.createTable.bind(WorkerQueue),
        ],function(err) {callback(err);});        
      } else callback();    
    },

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
    handleImportExport: ["config",handleImportExport]
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
