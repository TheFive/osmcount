var debug = require('debug')('WorkerQueue');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config         = require('../configuration.js');
var importCSV      = require('../ImportCSV.js');
var util           = require('../util.js');
var postgresMapper = require('../model/postgresMapper.js');


function WorkerQueue() {
  debug('WorkerQueue');
  this.databaseType = "postgres"; 
  this.tableName = "workerqueue";
  this.collectionName = "WorkerQueue";
  this.createTableString =
    'CREATE TABLE workerqueue (id bigserial primary key,measure text,key text,stamp timestamp with time zone, \
          status text,  exectime timestamp with time zone,type text,query text,source text,prio integer,error hstore) \
        WITH ( \
          OIDS=FALSE \
        ); '
  this.map= {
    tableName:"workerqueue",
    regex:{schluessel:true},
    keys: {schluessel:"key",
           source:'source',
           measure:'measure',
           status:'status',
           type:'type',
           timestamp:'stamp',
           id:'id'
         }
}
}

WorkerQueue.prototype.dropTable = postgresMapper.dropTable;
WorkerQueue.prototype.createTable = postgresMapper.createTable;
WorkerQueue.prototype.initialise = postgresMapper.initialise;
WorkerQueue.prototype.count = postgresMapper.count;




WorkerQueue.prototype.importCSV =function(filename,defJson,cb) {
  debug('exports.importCSV');
  console.log("No importCSV implemented for WorkerQueue, try import JSON");
  cb("No importCSV implemented",null);
}

function insertDataToPostgres (data,cb) {
  debug('insertDataToPostgres');
  should.exist(cb);
  debug('Connect String:'+config.postgresConnectStr);
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      cb(err);
      return;
    }
    var result = "Datensätze: "+data.length;
    var bar;
    if (data.length>100) {
      bar = new ProgressBar('Importing WorkerQueue: [:bar] :percent :total :etas', { total: data.length });
    }

    debug("Start insert "+ data.length + " datasets");
    function insertData(item,callback){
      debug('insertDataToPostgres->insertData');
      var key = item.schluessel;
      var stamp = item.timestamp;
      var measure = item.measure;
      var status = item.status;
      var exectime = item.exectime;
      var type = item.type;
      var query = item.query;
      var source = item._id;
      var error = util.toHStore(item.error)


      client.query("INSERT into workerqueue (key,stamp,measure,status,exectime,type,query,source,error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                          [key,stamp,measure,status,exectime,type,query,source,error], function(err,result) {
        if (err) {
          err.item = JSON.stringify(item);
          err.itemError = error;
        }
        if (bar) bar.tick();

        callback(err);

        if (err) debug('Error after Insert'+err);
      })
    }
    async.each(data,insertData,function(err) {pgdone();cb(err,result);})
  })

}

function exportMongoDB(filename,cb) {
  debug("exportMongoDB("+filename+")");
  should.exist(cb);
  var db = config.getMongoDB();
  var collection = db.collection('WorkerQueue');
  collection.find({},function exportsMongoDBCB1(err,data) {
    debug('exportsMongoDBCB1');
    if (err) {
      console.log("exportMongoDB Error: "+err);
      cb(err);
      return;
    }
    fs.writeFileSync(filename,"[");
    var delimiter="";
    var count=0;
    data.each(function (err,doc) {
      if (err) {
        cb(err);
        return
      }
      if (doc) {
        fs.appendFileSync(filename,delimiter);
        delimiter=",";
        count++;
        delete doc.data;
        fs.appendFileSync(filename,JSON.stringify(doc)+'\n');
      } else {
        fs.appendFileSync(filename,"]");
        console.log("DataLength to Export: "+count);
        console.log(filename +" is exported");
        cb();
      }
    })
  })
}

function importPostgresDB(filename,cb) {
  debug('importPostgresDB')
  debug('Filename %s',filename);
  try {
    data = fs.readFileSync(filename);
  } catch (err) {
    cb(err);
    return;
  }
  newData = JSON.parse(data);
  insertDataToPostgres(newData,cb);

}

WorkerQueue.prototype.import = function(filename,cb) {
  debug('exports.import');
  // for now, only postgres import is needed
  should(this.databaseType).equal('postgres');
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
WorkerQueue.prototype.export = function(filename,cb){
  debug('exports.export')
  // for now only mongo export is needed
  should(this.databaseType).equal('mongo');
  exportMongoDB(filename,cb);
}

WorkerQueue.prototype.insertData = function(data,cb) {
  debug('exports.insertData');
  if (this.databaseType == "mongo") {
    assert.equal("mongodb not implemented yet",null);
  }
  if (this.databaseType == "postgres") {
    insertDataToPostgres(data,cb);
  }
}



function getWorkingTaskMongoDB(cb) {
  debug('getWorkingTaskMongoDB');
  var db=config.getMongoDB();
  var collectionName = 'WorkerQueue';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.findOne({status:"working"},cb);
}

function getNextTaskPostgresDB(status,cb) {
  debug('getNextOpenTaskPostgresDB');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pddone();
      return;
    }

    var query = client.query("select id,key,stamp,measure,status,exectime,type,query,source  \
                         from workerqueue \
                         where status = $1 \
                           and exectime <= now() \
                          order by prio desc limit 1",
                        [status]);
    query.on('row',function(row) {
      var result = {};
      result.schluessel = row.key;
      result.timestamp = row.stamp;
      result.measure = row.measure;
      result.status = row.status;
      result.exectime = row.exectime;
      result.type = row.type;
      result.query = row.query;
      result.source = row.source;
      result.id = row.id;
      pgdone();
      cb(null,result);
      return;
    })
    query.on('end',function(result){
      if(result.rowCount==0) {
        cb(null,null);
        pgdone();
        return;
      }
    })
  })
}
WorkerQueue.prototype.getWorkingTask = function (cb) {
  debug('getWorkingTask');
  if (this.databaseType == "mongo") {
    getWorkingTaskMongoDB(cb);  
  }
  if (this.databaseType == "postgres") {
    getNextTaskPostgresDB("working",cb);
  }
  

}

function getNextOpenTaskMongoDB(cb) {
  debug('getWorkingTaskMongoDB');
  var db=config.getMongoDB();
  var collectionName = 'WorkerQueue';
  var date= new Date();
  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.findOne({ status : "open" ,
               exectime: {$lte: date}
              },
              {
                "sort": [['prio','desc']]
              },cb);
}

WorkerQueue.prototype.getNextOpenTask = function getNextOpenTask(cb) {
  debug('getNextOpenTask');
  if (this.databaseType == "mongo") {
    getNextOpenTaskMongoDB(cb);  
  }
  if (this.databaseType == "postgres") {
    getNextTaskPostgresDB("open",cb);
  }
}

function saveTaskMongoDB(task,cb) {
  debug('saveTaskMongoDB');
  var db=config.getMongoDB();
  db.collection("WorkerQueue").save(task,{w:1}, cb);
}

function saveTaskPostgresDB(task,cb) {
  debug('saveTaskPostgresDB');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    var key = task.schluessel;
    var stamp = task.timestamp;
    var measure = task.measure;
    var status = task.status;
    var exectime = task.exectime;
    var type = task.type;
    var query = task.query;
    var source = task._id;
    var id = task.id;


    client.query("update workerqueue SET (key,stamp,measure,status,exectime,type,query,source)  \
                                         = ($1,$2,$3,$4,$5,$6,$7,$8) \
                                         WHERE id = $9",
                        [key,stamp,measure,status,exectime,type,query,source,id], function(err,result) {
      should.not.exist(err);
      debug('Saved Rows %s with id %s status %s',result.rowCount,id,status);
      pgdone();
      cb(err);
      return;
    })
  })
}

WorkerQueue.prototype.saveTask = function(task,cb) {
  debug('saveTask');
  should.exist(cb);
  if (this.databaseType == 'mongo') {
    saveTaskMongoDB(task,cb);
  }
  if (this.databaseType == 'postgres') {
    saveTaskPostgresDB(task,cb);
  }
}

function findMongoDB(query,options,cb) {
  debug('findMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'WorkerQueue';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.find(query,options).toArray(cb);
}



function findPostgresDB(query,options,cb) {
  debug('findPostgresDB');
  postgresMapper.find(map,query,options,cb);
}

WorkerQueue.prototype.find = function(query,options,cb) {
  debug('find');
  if (this.databaseType == 'mongo') {
   findMongoDB(query,options,cb);
  }
  if (this.databaseType == "postgres") {
    findPostgresDB (query,options,cb);
  }
}

module.exports = new WorkerQueue();
