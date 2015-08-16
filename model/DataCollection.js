var debug = require('debug')('DataCollection');
var pg          = require('pg');
var fs          = require('fs');
var async       = require('async');
var should      = require('should');
var LRU         = require('lru-cache');
  
var JSONStream  = require('JSONStream');
var  es         = require('event-stream');

var util           = require('../util.js');
var config         = require('../configuration.js');
var importCSV      = require('../ImportCSV.js');
var postgresMapper = require('../model/postgresMapper.js');


function DataCollectionClass() {
  debug('DataCollectionClass');
  this.tableName = "DataCollection";
  this.collectionName = "DataCollection";
  var lruOptions = { max: 100000
                     , length: function (n) { return n.length }};
  this.aggregateCache=LRU(lruOptions);
  this.aggregateCacheKeys = {};

  this.createTableString = 'CREATE TABLE datacollection \
              (measure text, \
               key text, \
               stamp timestamp with time zone, \
               count integer,  \
               missing hstore, \
               existing hstore, \
               source character(64)) WITH ( OIDS=FALSE ); ';
  this.createIndexString = 'CREATE INDEX datacollection_stamp_idx ON datacollection  \
                              USING btree (stamp); \
                            CREATE INDEX datacollection_measure_key_idx ON datacollection  \
                               USING btree (measure COLLATE pg_catalog."default", key COLLATE pg_catalog."default");'

                            
  this.map={tableName:'datacollection',
         regex:{schluessel:true},
         hstore: ["missing","existing"],
         keys:{
          schluessel:'key',
          source:'source',
          measure:'measure',
          timestamp:'stamp',
          count:'count'
         }}
}

DataCollectionClass.prototype.dropTable = postgresMapper.dropTable;
DataCollectionClass.prototype.createTable = postgresMapper.createTable;
DataCollectionClass.prototype.initialise = postgresMapper.initialise;
DataCollectionClass.prototype.count = postgresMapper.count;
DataCollectionClass.prototype.import = postgresMapper.import;
DataCollectionClass.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
DataCollectionClass.prototype.insertData = postgresMapper.insertData;
DataCollectionClass.prototype.export = postgresMapper.export;
DataCollectionClass.prototype.find = postgresMapper.find;





DataCollectionClass.prototype.importCSV =function(filename,defJson,cb) {
  debug('DataCollectionClass.prototype.importCSV');
  var me = this;
  async.auto({
    loadData: function(callback) {
      importCSV.importCSVFileToJSON(filename,defJson,callback);
    },
    postgres: ["loadData",function(callback,asyncresult) {
      var newData = asyncresult.loadData;
      me.insertData(newData,callback);
    }]},
    function(err,asyncresult) {
      var result;
      result = asyncresult.postgres;
      cb(err,result);
    }
  );
}

DataCollectionClass.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into datacollection (key,stamp,measure,count,missing,existing) VALUES($1,$2,$3,$4,$5,$6)";
}

DataCollectionClass.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var key = item.schluessel;
  var timestamp = item.timestamp;
  var measure = item.measure;
  var count = item.count;
  var missing  = util.toHStore(item.missing);
  var existing = util.toHStore(item.existing);
  for (var k in item.existing) {
    if (existing != "" ) existing += ",";
    existing += '"' + k + '"=>"' +item.existing[k] + '"';
  }
  return  [key,timestamp,measure,count,missing,existing];
}



DataCollectionClass.prototype.invalidateCache =function invalidateCache(item) {
  debug('DataCollection.invalidateCache');
  debug('Invalidating: %s',item.measure);

  // Deleta all Keys belonging to measure from cache;
  var list = this.aggregateCacheKeys[item.measure];
  if (typeof(list)!='undefined') {
    for (var i=0;i<list.length;i++) {
      this.aggregateCache.del(list[i]);
    }
    this.aggregateCacheKeys[item.measure]=[];
  }
}

function aggregatePostgresDB(object,param,cb) {
  debug('aggregatePostgresDB');

  // is there somwhatin cache ?
  var result = [];
  var paramSinceDate = new Date(2000,0,1);
  var paramUpToDate  = new Date(2999,0,1);
  var paramLocation = param.location;
  if (typeof(paramLocation)=='undefined') {
    paramLocation = '';
  }
  paramLocation += '%';
/*  var paramLocationLength = paramLocation.length;*/
  if (param.since != '' && typeof(param.since)!='undefined') {
    paramSinceDate = new Date(param.since);
  }
  if (param.upTo != '' && typeof(param.upTo) != 'undefined')  {
    paramUpToDate = new Date(param.upTo);
  }
  var paramTimeFormat = 'YYYY-MM-DD';
  if (param.lengthOfTime == 4) {
    paramTimeFormat = 'YYYY';
  }
  if (param.lengthOfTime == 7) {
    paramTimeFormat = 'YYYY-MM';
  }
  var cellCalculation = "sum(count) as cell";
  var bindParam = [param.lengthOfKey,
              paramTimeFormat,
              param.measure,
              paramSinceDate,
              paramUpToDate,
            /*  paramLocationLength,
              paramLocation*/];
  if (typeof(param.sub)=='undefined') param.sub ='';
  if (typeof(param.subPercent)=='undefined') param.subPercent = "no";
  if (param.sub.match(/missing*/)) {
    var value = param.sub.substr(8,99);
    cellCalculation = "sum((missing->'"+value+"')::int) as cell"
    if (param.subPercent == "Yes") {
      cellCalculation = "case when sum(count)=0 then 1 else (sum((missing->'"+value+"')::float))/sum(count) end as cell";
    }
  }
   if (param.sub.match(/existing*/)) {
    var value = param.sub.substr(9,99);
    cellCalculation =  "sum((existing->'"+value+"')::int) as cell";
    if (param.subPercent == "Yes") {
      cellCalculation = "case when sum(count)=0 then 1 else (sum((existing->'"+value+"')::float))/sum(count) end as cell";
    }
  }
  var queryStr = "SELECT substr(key,1,$1) as k,\
            to_char(stamp,$2) as t," 
            + cellCalculation + 
            " from datacollection where measure = $3 \
               and stamp in (select distinct on (to_char(stamp,$2)) stamp from \
                             datacollection where measure = $3 and stamp >= $4  and stamp <= $5\
                              order by to_char(stamp,$2),stamp desc) \
               and key like '"+paramLocation+"' \
            group by k,t order by t;"
  var cacheKey = JSON.stringify(bindParam);
  cacheKey += cellCalculation;
  cacheKey += paramLocation;
  var cachedResult = object.aggregateCache.get(cacheKey);
  if (typeof(cachedResult)!='undefined') {
    cb(null,cachedResult);
    return;
  }


  pg.connect(config.postgresConnectStr,function(err,client,pgdone){
    debug('aggregatePostgresDB->CB');
    if (err) {
      console.log(err);
      cb(err);
      pgdone();
      return;
    }

    var query = client.query(queryStr,bindParam);
    query.on('error', function(err){
      console.log(err);
      pgdone();
      cb(err,null);
    });
    query.on('row',function aggregatePostgresDBQueryOnRow(row){
      var r = {};
      r._id = {};
      r._id.row = row.k;
      r._id.col = row.t;
      r.cell = row.cell;
      if (!(row.cell)) r.cell = 0;
      r.cell = parseFloat(r.cell);

      result.push(r);
    });
    query.on('end',function aggregatePostgresDBQueryOnEnd () {
      pgdone();
      if (typeof(object.aggregateCacheKeys[param.measure])=='undefined') {
        object.aggregateCacheKeys[param.measure] = [];
      }
      object.aggregateCache.set(cacheKey, result);
      object.aggregateCacheKeys[param.measure].push(cacheKey);
      cb(null,result);
    })
  });
}


/*
the aggregate Function accepts the following parameters
valueToCount: String can be every field, that was collected by
              overpass Query.
subPercent:   if yes, valueToCount is devided by count.
since:        date, only return results that are later
upTo:         date, only return results, that are earlier
lengthOfKey:  lenghtOfKey for group clause,
lengthOfTime: lenghtOfTime for group clause,
location:     key to filter on
*/

DataCollectionClass.prototype.aggregate = function(param,cb) {
  debug('DataCollectionClass.prototype.aggregate');
  aggregatePostgresDB(this,param,cb);
}


DataCollectionClass.prototype.savePostgresDB = function(data,cb) {
  debug('savePostgresDB');
  should.not.exist(data.id);
  this.insertData([data],cb);
}
DataCollectionClass.prototype.save = function(data,cb) {
  debug('DataCollectionClass.prototype.save');
    // invalidate Cache
  this.invalidateCache[data];
  this.savePostgresDB(data,cb);
}







module.exports  = new DataCollectionClass();

