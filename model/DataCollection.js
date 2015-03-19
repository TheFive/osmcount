var debug = require('debug')('DataCollection');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');

var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');

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
    client.query("DROP TABLE IF EXISTS DataCollection",function(err){
      debug("DataCollection Table Dropped");
      cb(err)
    });

    pgdone();
  })  
}

exports.createTable = function(cb) {
  debug('exports.createTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    client.query(postgresDB.createTableString,function(err) {
      debug('DataCollection Table Created');
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
  if (databaseType == "mongo") {
    importCSV.readCSVMongoDB(filename,defJson,cb);
    return;
  }
  if (databaseType == "postgres") {
    importCSV.readCSVPostgresDB(filename,defJson,cb);
  }
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
    function insertData(item,callback){
      debug('insertDataToPostgres->insertData');
      var key = item.schluessel;
      var timestamp = item.timestamp;
      var measure = item.measure;
      var count = item.count;
      var missing = "";
      for (var k in item.missing) {
        if (missing != "" ) missing += ",";
        missing += '"' + k + '"=>"' +item.missing[k] + '"';
      }
      var existing = "";
      for (var k in item.existing) {
        if (existing != "" ) existing += ",";
        existing += '"' + k + '"=>"' +item.existing[k] + '"';
      }
      client.query("INSERT into datacollection (key,stamp,measure,count,missing,existing) VALUES($1,$2,$3,$4,$5,$6)",
                          [key,timestamp,measure,count,missing,existing], function(err,result) {
        callback(err);
        debug('Error after Insert'+err);
      })
    }
    async.each(data,insertData,function(err) {pgdone();cb(err,result);})
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

function importPostgresDB(filename,cb) {
  debug('importPostgresDB')
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      if (cb) {
  			 cb(err);
  		} else {
  			 throw (err);
  		}
  		return;
  	}
    data = fs.readFileSync(filename);
    newData = JSON.parse(data);
    function insertData(item,callback){
  		var key = item.schluessel;
  		var timestamp = item.timestamp;
  		var measure = item.measure;
  		var count = item.count;
      var source = item.source;
  		var missing = "";
  		for (var k in item.missing) {
  			if (missing != "" ) missing += ",";
  			missing += '"' + k + '"=>"' +item.missing[k] + '"';
  		}
  		var existing = "";
  		for (var k in item.existing) {
  			if (existing != "" ) existing += ",";
  			existing += '"' + k + '"=>"' +item.existing[k] + '"';
  		}
  		client.query("INSERT into datacollection (key,timestamp,measure,count,missing,existing,source) VALUES($1,$2,$3,$4,$5,$6,$7)",
  					                    [key,timestamp,measure,count,missing,existing,source], function(err,result) {
  	    callback(err);
  		})
  	}
  	async.each(newData,insertData,function(err) {pgdone();cb(err);})
  });
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
