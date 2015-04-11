var debug = require('debug')('DataCollection');
var pg          = require('pg');
var fs          = require('fs');
var async       = require('async');
var should      = require('should');
var ProgressBar = require('progress');

var JSONStream  = require('JSONStream');
var  es         = require('event-stream');

var util           = require('../util.js');
var config         = require('../configuration.js');
var importCSV      = require('../ImportCSV.js');
var postgresMapper = require('../model/postgresMapper.js')

var databaseType = "postgres";

var postgresDB = {
  createTableString :
    'CREATE TABLE datacollection (measure text,key text,stamp timestamp with time zone, \
          count integer,  missing hstore,existing hstore,source character(64)) \
        WITH ( \
          OIDS=FALSE \
        ); '
}

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    client.query("DROP TABLE IF EXISTS DataCollection",function(err){
      debug("DataCollection Table Dropped");
      cb(null)
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
      debug('DataCollection Table Created');
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
  if (databaseType == "mongo") {
    importCSV.readCSVMongoDB(filename,defJson,cb);
    return;
  }
  if (databaseType == "postgres") {
    importCSV.readCSVPostgresDB(filename,defJson,cb);
  }
}

function insertStreamToPostgres (internal,stream,cb) {
  debug('insertStreamToPostgres');
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

    var counter = 0;
    var bar;
    var versionDefined = false;

    function insertData(item,callback){
      debug('insertStreamToPostgres->insertData');
      if (!internal) {
        if (typeof(item.collection) == 'string') {
          if (item.collection == "DataCollection") {
            should(item.version).equal(1,"Import Version is not equal 1");
            versionDefined = true;
            bar = new ProgressBar('Importing DataCollection: [:bar] :percent :current :total :etas', { total: item.count });
            callback();
            return;
          }
        }
        should.ok(versionDefined,"No Version Number and FileType in File");    
      }
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
      var query = client.query("INSERT into datacollection (key,stamp,measure,count,missing,existing) VALUES($1,$2,$3,$4,$5,$6)",
                          [key,timestamp,measure,count,missing,existing]);
      query.on("error",function(err){
        debug('Error after Insert'+err);
        err.item = item;
        callback(err);
      })
      query.on("end",function(){        
        counter = counter +1;
        debug("query.end was called");
        if (bar) bar.tick();
        callback();
      })
      // Undocumneted ? 
      // Create Backpressure to reduce write speed...
      return false;
    }
    var ls, mapper;
    var parser;
    if (internal) {
      ls = stream.pipe(es.map(insertData));
      parser = ls;
      mapper = ls;
    }
    else {
      parser = JSONStream.parse();

      var mapper = es.map(insertData);
      ls = stream.pipe(parser).pipe(mapper);
    }
    parser.on('end',function() {debug("parser.on('end');")})
    stream.on('end',function() {debug("stream.on('end');")})
    mapper.on('end',function() {debug("mapper.on('end');")})
    ls.on('end',function() {
      debug("ls.on('end')");

      // Quickhack  because is called two times
      if (!this.wasCalled) cb(null,"Datensätze: "+counter);
      this.wasCalled = true;
    })
    ls.on('error',function(err) {
      debug("ls.on('error);")

      cb(err);
    })


  })

}

function aggregatePostgresDB(param,cb) {
  debug('aggregatePostgresDB');
  var result = [];
  var paramSinceDate = new Date(2000,0,1);
  var paramUpToDate  = new Date(2999,0,1);
  var paramLocation = param.location;
  if (typeof(paramLocation)=='undefined') {
    paramLocation = '';
  }
  var paramLocationLength = paramLocation.length;
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


  pg.connect(config.postgresConnectStr,function(err,client,pgdone){
    debug('aggregatePostgresDB->CB');
    if (err) {
      console.log(err);
      cb(err);
      pgdone();
      return;
    }
    var cellCalculation = "sum(count) as cell";
    var bindParam = [param.lengthOfKey,
                paramTimeFormat,
                param.measure,
                paramSinceDate,
                paramUpToDate,
                paramLocationLength,
                paramLocation];
    if (typeof(param.sub)=='undefined') param.sub ='';
    if (typeof(param.subPercent)=='undefined') param.subPercent = "no";
    if (param.sub.match(/missing*/)) {
      var value = param.sub.substr(8,99);
      cellCalculation = "sum((missing->'"+value+"')::int) as cell"
      if (param.subPercent == "Yes") {
        cellCalculation = "(sum((missing->'"+value+"')::float))/sum(count) as cell";
      }
    }
     if (param.sub.match(/existing*/)) {
      var value = param.sub.substr(9,99);
      cellCalculation =  "sum((existing->'"+value+"')::int) as cell";
      if (param.subPercent == "Yes") {
        cellCalculation = "(sum((existing->'"+value+"')::float))/sum(count) as cell";
      }
    }
    var queryStr = "SELECT substr(key,1,$1) as k,\
              to_char(stamp,$2) as t," 
              + cellCalculation + 
              " from datacollection where measure = $3 \
                 and stamp >= $4 \
                 and stamp <= $5 \
                 and stamp in (select distinct on (to_char(stamp,$2)) stamp from \
                               datacollection where measure = $3 order by to_char(stamp,$2),stamp desc) \
                 and substr(key,1,$6) = $7 \
              group by k,t;"

    var query = client.query(queryStr,bindParam);
    query.on('error', function(err){
      console.log(err);
      pgdone();
      cb(err,null);
    });
    query.on('row',function(row){
      var r = {};
      r._id = {};
      r._id.row = row.k;
      r._id.col = row.t;
      r.cell = row.cell;
      if (!(row.cell)) r.cell = 0;
      r.cell = parseFloat(r.cell);

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
  var collection = db.collection('DataCollection');
  var valueToCount = param.valueToCount;
  var valueToDisplay = "$count";
  var paramSinceDate = new Date(1900,0,1);
  var paramUpToDate  = new Date(2999,0,1);

  if (param.subPercent == "Yes") {
    valueToDisplay = { $cond : [ {$eq : ["$count",0]},0,{$divide: [ "$count","$total"]}]};
    ranktype = "up";
  }
  if (param.since != '') paramSinceDate = new Date(param.since);
  if (param.upTo != '')  paramUpToDate = new Date(param.upTo);

  var preFilter = {$match: { $and: [{timestamp: {$gte: paramSinceDate}},
                                    {timestamp: {$lte: paramUpToDate}}],
                             measure: param.measure,
                             schluessel: {$regex: "^"+param.location}}};

var projection = {$project:{measure:1,
                            timestamp:1,
                            schluessel:1,
                            count:1,
                            source:1,
                            "missing.name":1,
                            "missing.wheelchair":1,
                            "missing.phone":1,
                            "missing.opening_hours":1,
                            "existing.fixme":1}};

var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
                     timestamp: "$timestamp",
                     timestampShort: {$substr: ["$timestamp",0,param.lengthOfTime]},
                     count: valueToCount,
                     total: "$count",
                     source: "$source"
                     }};
 var aggregateMeasuresGroup = {$group: { _id: { row: "$schluessel",source:"$source"},
                     count	: {$sum: "$count" },
                     total	: {$sum: "$total" },
                     timestamp:{$last:"$timestamp"},
                     schluessel: {$last:"$schluessel"},
                     timestampShort: {$last:"$timestampShort"}}};

var presort = {$sort: { timestamp:1}};

var aggregateTimeAxisStep2 = {$group: { _id: { row: "$schluessel",col:"$timestampShort"},
                     cell	: {$last: valueToDisplay }}};




  var sort = {$sort: { _id:1}};

  var query = [projection,
               preFilter,
         aggregateMeasuresProj,
         aggregateMeasuresGroup,
         presort,
         aggregateTimeAxisStep2,
         sort];
  console.dir(JSON.stringify(preFilter));
   collection.aggregate(query,cb);

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

function exportMongoDB(filename,cb) {
  debug("exportMongoDB("+filename+")");
  var db = config.getMongoDB();
  var collection = db.collection('DataCollection');
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
        return;
      }
      if (doc) {
        fs.appendFileSync(filename,delimiter);
        delimiter=",";
        count++;
        delete doc.data;
        fs.appendFileSync(filename,JSON.stringify(doc)+'\n');
      } else {
        fs.appendFileSync(filename,"]");
        console.log(filename +" is exported with "+count+" datasets.");
        if (cb) cb();
      }
    })
  })
}

 
 


 

var getStream = function (filename) {
    var stream = fs.createReadStream(filename, {encoding: 'utf8'});
        return stream;
};



function importPostgresDBStream(filename,cb) {
  debug('importPostgresDBStream');


  var stream = getStream(filename);


  insertStreamToPostgres(false,stream,cb);
}

function importPostgresDB(filename,cb) {
  debug('importPostgresDB')
/*  var data = fs.readFileSync(filename);
  var newData = JSON.parse(data);
  insertDataToPostgres(newData,cb);*/
  importPostgresDBStream(filename,cb);
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
    should.exist(null,"mongodb not implemented yet");
  }
  if (databaseType == "postgres") {

    // Turn Data into a stream
    var reader = es.readArray(data);

    // use Stream Function to put data to Postgres
    insertStreamToPostgres(true,reader,cb);
  }
}
exports.saveMongoDB =function(data,cb) {
  var db = config.getMongoDB();
  db.collection("DataCollection").save(data,{w:1}, cb);
}

exports.savePostgresDB = function(data,cb) {
  debug('savePostgresDB');
  should.not.exist(data.id);
  exports.insertData([data],cb);
}
exports.save = function(data,cb) {
  debug('exports.save');
  if (databaseType == "mongo") {
    exports.saveMongoDB(data,cb);
  }
  if (databaseType == "postgres") {
    exports.savePostgresDB(data,cb);
  }
}

var map={tableName:'datacollection',
         regex:{schluessel:true},
         keys:{
          schluessel:'key',
          source:'source',
          measure:'measure',
          timestamp:'stamp',
          count:'count'
         }}

function countPostgresDB(query,cb) {
  debug('countPostgresDB');
  postgresMapper.count(map,query,cb);

}

function countMongoDB(query,cb) {
  debug('countMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'DataCollection';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.count(query, cb);
}

exports.count = function(query,cb) {
  debug('count');
  if (databaseType == "mongo") {
    countMongoDB(query,cb);
  }
  if (databaseType == "postgres") {
    countPostgresDB(query,cb);
  }
}

function findMongoDB(query,options,cb) {
  debug('findMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'DataCollection';

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

