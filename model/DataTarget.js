var debug  = require('debug')('DataTarget');
var pg     = require('pg');
var fs     = require('fs');
var async  = require('async');
var should = require('should');
var ProgressBar = require('progress');
var JSONStream  = require('JSONStream');
var  es         = require('event-stream');



var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var postgresMapper = require('../model/postgresMapper.js')








function DataTargetClass() {
  debug('DataTarget');
  this.databaseType = "postgres"; 
  this.tableName = "DataTarget";
  this.collectionName = "DataTarget";
  this.createTableString = "CREATE TABLE datatarget ( key text, measure text, target double precision, \
      name text, sourceText text, sourceLink text, id bigserial NOT NULL, \
      CONSTRAINT id_datatarget PRIMARY KEY (id) )"
  this.map= {
    tableName:"datatarget",
    regex:{schluessel:true},
    keys: {schluessel:"key",
           measure:'measure',
           apothekenVorgabe: 'target',
           name: 'name',
           source:'sourcetext',
           sourceLink:'sourcelink',
           id:'id'
         }}
}

DataTargetClass.prototype.dropTable = postgresMapper.dropTable;
DataTargetClass.prototype.createTable = postgresMapper.createTable;
DataTargetClass.prototype.initialise = postgresMapper.initialise;
DataTargetClass.prototype.count = postgresMapper.count;
DataTargetClass.prototype.import = postgresMapper.import;
DataTargetClass.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
DataTargetClass.prototype.export = postgresMapper.export;
DataTargetClass.prototype.find = postgresMapper.find;





DataTargetClass.prototype.importCSV =function(filename,defJson,cb) {
  debug('DataTarget.prototype.importCSV');
  console.log("No importCSV implemented for datatarget, try import JSON");
  cb("No importCSV implemented",null);
}



DataTargetClass.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into datatarget (key,measure,target,name,sourceText,sourceLink) VALUES($1,$2,$3,$4,$5,$6)";
}

DataTargetClass.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var key = item.schluessel;
  var measure = item.measure;
  var target = item.apothekenVorgabe;
  var name = item.name;
  var sourceText = item.source;
  var sourceLink = item.linkSource;
  return  [key,measure,target,name,sourceText,sourceLink];
}





DataTargetClass.prototype.insertData = function(data,cb) {
  debug('DataTarget.prototype.insertData');
  if (this.databaseType == "mongo") {
    should.exist(null,"mongodb not implemented yet");
  }
  if (this.databaseType == "postgres") {

    // Turn Data into a stream
    var reader = es.readArray(data);

    // use Stream Function to put data to Postgres
    insertStreamToPostgres(true,reader,cb);
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

DataTargetClass.prototype.aggregate = function(param,cb) {
  debug('DataTarget.prototype.aggregate');
  if (this.databaseType == "mongo") {
    aggregateMongoDB(param,cb);
    return;
  }
  if (this.databaseType == "postgres") {
    aggregatePostgresDB(param,cb);
  }
}








module.exports = new DataTargetClass();


