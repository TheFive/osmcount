var debug = require('debug')('WorkerQueue');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');
var should = require('should');

var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');

var databaseType = "postgres";

var postgresDB = {
  createTableString :
    'CREATE TABLE workerqueue (id bigserial primary key,measure text,key text,stamp timestamp with time zone, \
          status text,  exectime timestamp with time zone,type text,query text,source text) \
        WITH ( \
          OIDS=FALSE \
        ); '
}

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    client.query("DROP TABLE IF EXISTS WorkerQueue",function(err){
      debug("WorkerQueue Table Dropped");
      cb(err)
    });

    pgdone();
  })  
}

exports.createTable = function(cb) {
  debug('exports.createTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    client.query(postgresDB.createTableString,function(err) {
      debug('WorkerQueue Table Created');
      cb(err);
    });
    pgdone();
  })
} 

exports.initialise = function initialise(dbType,callback) {
  debug('exports.initialise');
  if (dbType) {
    databaseType = dbType;
  }
  else {
    databaseType = config.getDatabaseType();
  }
  if (callback) callback();
}

exports.importCSV =function(filename,defJson,cb) {
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
     

      client.query("INSERT into workerqueue (key,stamp,measure,status,exectime,type,query,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                          [key,stamp,measure,status,exectime,type,query,source], function(err,result) {
        callback(err);
        debug('Error after Insert'+err);
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
  data = fs.readFileSync(filename);
  newData = JSON.parse(data);
  insertDataToPostgres(newData,cb);
 
}

exports.import = function(filename,cb) {
  debug('exports.import')
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
exports.export = function(filename,cb){
  debug('exports.export')
   exportMongoDB(filename,cb);
}

exports.insertData = function(data,cb) {
  debug('exports.insertData');
  if (databaseType == "mongo") {
    assert.equal("mongodb not implemented yet",null);
  }
  if (databaseType == "postgres") {
    insertDataToPostgres(data,cb);
  }
}

function countMongoDB(query,cb) {
  debug('countMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'WorkerQueue';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.count(query, cb);
}

function countPostgresDB(query,cb) {
  debug('countPostgresDB');


  var whereClause = ""
  if (typeof(query.schluessel)!= 'undefined') {
    whereClause = "key ~ '"+query.schluessel+"'";
  } 
  if (typeof(query.source) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "source = '"+query.source+"'";
  }
  if (typeof(query.measure) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "measure = '"+query.measure+"'";
  }
  if (typeof(query.status) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "status = '"+query.status+"'";
  }
  if (typeof(query.type) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "type = '"+query.type+"'";
  }
  if (typeof(query.timestamp) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "stamp = '"+query.timestamp+"'";
  }
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    should.not.exist(err);
    client.query("select count(*) from workerqueue where "+whereClause,
                                function(err,result) {
      
      if (!err) {
        var count = result.rows[0].count;
        cb (null,count);
        pgdone();
        return;
      }
      cb(err,null);
      pgdone();
      return;
    })
  })

}

exports.count = function(query,cb) {
  debug('count');
  if (databaseType == 'mongodb') {
   countMongoDB(query,cb);
  }
  if (databaseType == "postgres") {
    countPostgresDB (query,cb);
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
exports.getWorkingTask = function (cb) {
  debug('getWorkingTask');
  getWorkingTaskMongoDB(cb);
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

exports.getNextOpenTask = function getNextOpenTask(cb) {
  debug('getNextOpenTask');
  getNextOpenTaskMongoDB(cb);
}

 function saveTaskMongoDB(task,cb) {
  var db=config.getMongoDB();
  db.collection("WorkerQueue").save(task,{w:1}, cb);
}

exports.saveTask= function(task,cb) {
  debug('saveTask');
  saveTaskMongoDB(task,cb);
}
