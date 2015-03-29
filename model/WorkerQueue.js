var debug = require('debug')('WorkerQueue');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var util      = require('../util.js');

var databaseType = "postgres";

var postgresDB = {
  createTableString :
    'CREATE TABLE workerqueue (id bigserial primary key,measure text,key text,stamp timestamp with time zone, \
          status text,  exectime timestamp with time zone,type text,query text,source text,prio integer,error hstore) \
        WITH ( \
          OIDS=FALSE \
        ); '
}

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      pgdone();
      cb(err);
      return;
    }
    client.query("DROP TABLE IF EXISTS workerqueue",function(err){
      debug("WorkerQueue Table Dropped");
      pgdone();
      cb(err)
    });
  })
}

exports.createTable = function(cb) {
  debug('exports.createTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    debug('Call Create Table');
    debug(postgresDB.createTableString);
    client.query(postgresDB.createTableString,function(err) {
      debug('WorkerQueue Table Created');
      pgdone();
      cb(err);
    });
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
  debug('Database Type is now %s',databaseType);
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

exports.import = function(filename,cb) {
  debug('exports.import');
  // for now, only postgres import is needed
  should(databaseType).equal('postgres');
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
exports.export = function(filename,cb){
  debug('exports.export')
  // for now only mongo export is needed
  should(databaseType).equal('mongo');
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
  if (typeof(query.id) != 'undefined') {
    if (whereClause != '') whereClause += " and ";
    whereClause += "id = '"+query.id+"'";
  }
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    client.query("select count(*) from workerqueue where "+whereClause,
                                function(err,result) {
      should.not.exist(err);
      var count = result.rows[0].count;
      cb (null,count);
      pgdone();
      return;
    })
  })

}

exports.count = function(query,cb) {
  debug('count');
  if (databaseType == 'mongo') {
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
exports.getWorkingTask = function (cb) {
  debug('getWorkingTask');
  if (databaseType == "mongo") {
    getWorkingTaskMongoDB(cb);  
  }
  if (databaseType == "postgres") {
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

exports.getNextOpenTask = function getNextOpenTask(cb) {
  debug('getNextOpenTask');
  if (databaseType == "mongo") {
    getNextOpenTaskMongoDB(cb);  
  }
  if (databaseType == "postgres") {
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

exports.saveTask = function(task,cb) {
  debug('saveTask');
  should.exist(cb);
  if (databaseType == 'mongo') {
    saveTaskMongoDB(task,cb);
  }
  if (databaseType == 'postgres') {
    saveTaskPostgresDB(task,cb);
  }
}
