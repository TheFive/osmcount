var debug = require('debug')('DataTarget');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');

var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');

var databaseType = "postgres";

var postgresDB = { 
  createTableString: 
  "CREATE TABLE datatarget ( key text, measure text, target double precision, \
      name text, sourceText text, sourceLink text, id bigserial NOT NULL, \
      CONSTRAINT id_datatarget PRIMARY KEY (id) )"
};

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
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
    client.query(postgresDB.createTableString,function(err) {
      debug('datatarget Table Created');
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
  console.log("No importCSV implemented for datatarget, try import JSON");
  cb("No importCSV implemented",null);
}

function insertDataToPostgres (data,cb) {
  debug('insertDataToPostgres');
  debug('Connect String:'+config.postgresConnectStr);
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      if (cb) {
        cb(err);
      } else {
        throw (err);
      }
      return;
    }
    var result = "Datensätze: "+data.length; 
    debug("Start insert "+ data.length + " datasets");
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
        callback(err);
        debug('Error after Insert'+err);
      })
    }
    async.each(data,insertData,function(err) {pgdone();cb(err,result);})
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


