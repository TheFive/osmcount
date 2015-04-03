var debug  = require('debug')('DataTarget');
var pg     = require('pg');
var fs     = require('fs');
var async  = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var postgresMapper = require('../model/postgresMapper.js')


var databaseType = "postgres";

var postgresDB = {
  createTableString:
  "CREATE TABLE datatarget ( key text, measure text, target double precision, \
      name text, sourceText text, sourceLink text, id bigserial NOT NULL, \
      CONSTRAINT id_datatarget PRIMARY KEY (id) )"
};

var map= {
  tableName:"datatarget",
  regex:{schluessel:true},
  keys: {schluessel:"key",
         measure:'measure',
         apothekenVorgabe: 'target',
         name: 'name',
         source:'sourcetext',
         sourceLink:'sourcelink',
         id:'id'
       }
}

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    client.query("DROP TABLE IF EXISTS datatarget",function(err){
      debug("datatarget Table Dropped");
      cb(err)
    });

    pgdone();
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
    client.query(postgresDB.createTableString,function(err) {
      debug('datatarget Table Created');
      cb(err);
    });
    pgdone();
  })
}

exports.initialise = function initialise(dbType,callback) {
  debug('exports.initialise');

  if (typeof(dbType) != 'function') {
    databaseType = dbType;
  }
  else {
    databaseType = config.getDatabaseType();
    callback = dbType;
  }
  if (databaseType == 'postgres') {
    postgresMapper.invertMap(map);
  }
  if (callback) callback();
}

exports.importCSV =function(filename,defJson,cb) {
  debug('exports.importCSV');
  console.log("No importCSV implemented for datatarget, try import JSON");
  cb("No importCSV implemented",null);
}

function insertDataToPostgres (data,cb) {
  debug('insertDataToPostgres');
  debug('Connect String:'+config.postgresConnectStr);
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      cb(err);
      return;
    }
    var result = "Datensätze: "+data.length;
    debug("Start insert "+ data.length + " datasets");
    var bar;
    if (data.length>100) {
      bar = new ProgressBar('Importing DataTarget: [:bar] :percent :total :etas', { total: data.length });
    }
    function insertData(item,callback){
      debug('insertDataToPostgres->insertData');
      var key = item.schluessel;
      var measure = item.measure;
      var target = item.apothekenVorgabe;
      var name = item.name;
      var sourceText = item.source;
      var sourceLink = item.linkSource;
      client.query("INSERT into datatarget (key,measure,target,name,sourceText,sourceLink) VALUES($1,$2,$3,$4,$5,$6)",
                          [key,measure,target,name,sourceText,sourceLink,], function(err,result) {
        if (err) {
          err.item = item;
        }
        if (bar) bar.tick();
        callback(err);
        debug('Error after Insert'+err);
      })
    }
    async.each(data,insertData,function(err) {
      pgdone();
      cb(err,result);
    })
  })

}



function exportMongoDB(filename,cb) {
  debug("exportMongoDB("+filename+")");
  var db = config.getMongoDB();
  var collection = db.collection('DataTarget');
  collection.find({},function exportsMongoDBCB1(err,data) {
    debug('exportsMongoDBCB1');
    if (err) {
      console.log("exportMongoDB Error: "+err);
      if (cb) cb(err);
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
        if (cb) cb();
      }
    })
  })
}

function importPostgresDB(filename,cb) {
  debug('importPostgresDB')
  debug('Filename %s',filename);
  try {
    data = fs.readFileSync(filename);
  }
  catch (err) {
    cb(err,null);
    return;
  }
  newData = JSON.parse(data);
  insertDataToPostgres(newData,cb);

}

exports.import = function(filename,cb) {
  debug('exports.import')
  should(databaseType).equal('postgres');
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
exports.export = function(filename,cb){
  debug('exports.export')
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


function aggregatePostgresDB(param,cb) {
  debug('aggregatePostgresDB');
  var result = [];

  var paramLocation = param.location;
  if (typeof(paramLocation)=='undefined') {
    paramLocation = '';
  }
  var paramLocationLength = paramLocation.length;


  pg.connect(config.postgresConnectStr,function(err,client,pgdone){
    debug('aggregatePostgresDB->CB');
    if (err) {
      console.log(err);
      cb(err);
      pgdone();
      return;
    }
    var cellCalculation = "sum(target) as cell";
    var bindParam = [param.lengthOfKey,
                param.measure,
                paramLocationLength,
                paramLocation];
    var queryStr = "SELECT substr(key,1,$1) as k,sum(target) as t \
                 from datatarget where measure = $2 \
                 and substr(key,1,$3) = $4 \
                 group by k";

    var query = client.query(queryStr,bindParam);
    query.on('error', function(err){
      console.log(err);
      pgdone();
      cb(err,null);
    });
    query.on('row',function(row){
      var r = {};
      r._id = row.k;
      r.vorgabe = row.t;
      result.push(r);
    });
    query.on('end',function() {
      pgdone();
      cb(null,result);
    })
  });
}

function aggregateMongoDB(param,cb) {
  debug('aggregateMongoDB');

  var db = config.getMongoDB();
  var collection = db.collection('DataTarget');
  var preFilterVorgabe = {$match: {
                              measure: param.measure,
                              schluessel: {$regex: "^"+param.location}}};



  var queryVorgabe = [
        preFilterVorgabe,
        {$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
                        vorgabe: "$apothekenVorgabe"
                        }},
          {$group: { _id:  "$schluessel",
                 vorgabe  : {$sum: "$vorgabe" },
                      }}];
  collection.aggregate(query,cb);

}
/*
the aggregate Function accepts the following parameters
lengthOfKey:  lenghtOfKey for group clause,
location:     key to filter on,
measure:      measure to work on,
*/

exports.aggregate = function(param,cb) {
  debug('exports.aggregate');
  if (databaseType == "mongo") {
    aggregateMongoDB(param,cb);
    return;
  }
  if (databaseType == "postgres") {
    aggregatePostgresDB(param,cb);
  }
}

function countMongoDB(query,cb) {
  debug('countMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'DataTarget';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.count(query, cb);
}



function countPostgresDB(query,cb) {
  debug('countPostgresDB');
  postgresMapper.count(map,query,cb);

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

function findMongoDB(query,options,cb) {
  debug('findMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'DataTarget';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.find(query,options).toArray(cb);
}



function findPostgresDB(query,options,cb) {
  debug('findPostgresDB');
  postgresMapper.find(map,query,options,cb);
}

exports.find = function(query,options,cb) {
  debug('find');
  if (databaseType == 'mongo') {
   findMongoDB(query,options,cb);
  }
  if (databaseType == "postgres") {
    findPostgresDB (query,options,cb);
  }
}


