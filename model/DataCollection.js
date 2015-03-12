var debug = require('debug')('DataCollection');
var pg    = require('pg');
var fs    = require('fs');
var async = require('async');

var config    = require('../configuration.js');
var importCSV = require('../importCSV.js');

var databaseType = "postgres";
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


function aggregatePostgresDB(param,cb) {
  debug('aggregatePostgresDB');
  var result = [];
  var paramSinceDate = new Date(1900,0,1);
  var paramUpToDate  = new Date(2999,0,1);
  if (param.since != '') paramSinceDate = new Date(param.since);
  if (param.upTo != '')  paramUpToDate = new Date(param.upTo);

  pg.connect(config.postgresConnectStr,function(err,client,pgdone){
    var query = client.query(
      "SELECT substr(key,1,$1) as key,\
              substr(to_char(timestamp,'YYYY-MM-DD'),1,$2) as timestamp,\
              sum(count) as cell from test \
              where measure = $3 \
                 and timestamp >= $4 \
                 and timestamp <= $5 \
              group by key,measure,timestamp",
              [param.lengthOfKey,
                param.lengthOfTime,
                param.measure,
                paramSinceDate,
                paramUpToDate]);
    query.on('row',function(row){
      var r = {};
      r._id = {};
      r._id.row = row.key;
      r._id.col = row.timestamp;
      r.cell = row.cell;

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
  console.log(paramSinceDate);
  console.log(paramUpToDate);
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
  		client.query("INSERT into test (key,timestamp,measure,count,missing,existing,source) VALUES($1,$2,$3,$4,$5,$6,$7)",
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
