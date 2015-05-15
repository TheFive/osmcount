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
  this.tableName = "WorkerQueue";
  this.collectionName = "WorkerQueue";
  this.createTableString =
    'CREATE TABLE workerqueue  \
      (id bigserial primary key, \
       measure text, \
       key text, \
       stamp timestamp with time zone, \
        status text, \
        exectime timestamp with time zone, \
        type text, \
        query text, \
        source text, \
        prio integer, \
        error hstore) \
        WITH ( \
          OIDS=FALSE \
        ); '
  this.createIndexString = 
     'CREATE INDEX workerqueue_status_prio_idx \
      ON workerqueue \
      USING btree \
      (status COLLATE pg_catalog."default", prio);'
  this.map= {
    tableName:"workerqueue",
    regex:{schluessel:true},
    hstore:["error"],
    keys: {schluessel:"key",
           source:'source',
           measure:'measure',
           status:'status',
           type:'type',
           timestamp:'stamp',
           exectime:'exectime',
           id:'id',
           query:'query',
           source:'source',
           prio:'prio'
         }
  }
}

WorkerQueue.prototype.dropTable = postgresMapper.dropTable;
WorkerQueue.prototype.createTable = postgresMapper.createTable;
WorkerQueue.prototype.initialise = postgresMapper.initialise;
WorkerQueue.prototype.count = postgresMapper.count;
WorkerQueue.prototype.countUntilNow = postgresMapper.countUntilNow;
WorkerQueue.prototype.import = postgresMapper.import;
WorkerQueue.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
WorkerQueue.prototype.insertData = postgresMapper.insertData;
WorkerQueue.prototype.export = postgresMapper.export;
WorkerQueue.prototype.find = postgresMapper.find;



WorkerQueue.prototype.importCSV =function(filename,defJson,cb) {
  debug('exports.importCSV');
  console.log("No importCSV implemented for WorkerQueue, try import JSON");
  cb("No importCSV implemented",null);
}

WorkerQueue.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into workerqueue (key,stamp,measure,status,exectime,type,query,source,error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)";
}

WorkerQueue.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var key = item.schluessel;
  var stamp = item.timestamp;
  var measure = item.measure;
  var status = item.status;
  var exectime = item.exectime;
  var type = item.type;
  var query = item.query;
  var source = item._id;
  var error = util.toHStore(item.error);
  return  [key,stamp,measure,status,exectime,type,query,source,error];
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
                          order by exectime, prio desc limit 1",
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
  getNextTaskPostgresDB("working",cb);
}


WorkerQueue.prototype.getNextOpenTask = function getNextOpenTask(cb) {
  debug('getNextOpenTask');
  getNextTaskPostgresDB("open",cb);
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
    var error = util.toHStore(task.error);
    var id = task.id;


    client.query("update workerqueue SET (key,stamp,measure,status,exectime,type,query,source,error)  \
                                         = ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
                                         WHERE id = $10",
                        [key,stamp,measure,status,exectime,type,query,source,error,id], function(err,result) {

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
  saveTaskPostgresDB(task,cb);
}



module.exports = new WorkerQueue();
