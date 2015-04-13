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








function DataTarget() {
  debug('DataTarget');
  this.databaseType = "postgres"; 
  this.tableName = "datatarget";
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

DataTarget.prototype.dropTable = postgresMapper.dropTable;
DataTarget.prototype.createTable = postgresMapper.createTable;
DataTarget.prototype.initialise = postgresMapper.initialise;
DataTarget.prototype.count = postgresMapper.count;




DataTarget.prototype.importCSV =function(filename,defJson,cb) {
  debug('DataTarget.prototype.importCSV');
  console.log("No importCSV implemented for datatarget, try import JSON");
  cb("No importCSV implemented",null);
}

function insertStreamToPostgres (internal,stream,cb) {
  debug('insertDataToPostgres');
  debug('Connect String:'+config.postgresConnectStr);
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      cb(err);
      return;
    }

    var counter = 0;
    var bar;
    var versionDefined = false;

    function insertData(item,callback){
      debug('insertStreamToPostgres->insertData');
      if (!internal) {
        if (typeof(item.collection) == 'string') {
          if (item.collection == "DataTarget") {
            should(item.version).equal(1,"Import Version is not equal 1");
            versionDefined = true;
            bar = new ProgressBar('Importing DataTarget: [:bar] :percent :current :total :etas', { total: item.count });
            callback();
            return;
          }
        }
        should.ok(versionDefined,"No Version Number and FileType in File");     
      }
      var key = item.schluessel;
      var measure = item.measure;
      var target = item.apothekenVorgabe;
      var name = item.name;
      var sourceText = item.source;
      var sourceLink = item.linkSource;
      var query = client.query("INSERT into datatarget (key,measure,target,name,sourceText,sourceLink) VALUES($1,$2,$3,$4,$5,$6)",
                          [key,measure,target,name,sourceText,sourceLink,]);
      query.on("error",function(err){
        debug('Error after Insert'+err);
        err.item = item;
        callback(err);
        cb(err);
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

    var pipe = stream.pipe(es.map(insertData));

    stream.on('end',function() {
      debug("parser.on('end')")
      //insertData({"result":result},cb)

      cb(null,"Datensätze: "+counter);
    })
    stream.on('error',function(err) {
      debug("parser.on('error);")

      cb(err);
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

var getStream = function (filename) {
    var stream = fs.createReadStream(filename, {encoding: 'utf8'});
        return stream;
};



function importPostgresDBStream(filename,cb) {
  debug('importPostgresDBStream');

  var stream = getStream(filename);
  var parser = JSONStream.parse();
  


  insertStreamToPostgres(false,stream.pipe(parser),cb);
}

function importPostgresDB(filename,cb) {
  debug('importPostgresDB')
/*  var data = fs.readFileSync(filename);
  var newData = JSON.parse(data);
  insertDataToPostgres(newData,cb);*/
  importPostgresDBStream(filename,cb);
}

DataTarget.prototype.import = function(filename,cb) {
  debug('DataTarget.prototype.import')
  should(this.databaseType).equal('postgres');
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
DataTarget.prototype.export = function(filename,cb){
  debug('DataTarget.prototype.export')
  should(this.databaseType).equal('mongo');
  exportMongoDB(filename,cb);
}

DataTarget.prototype.insertData = function(data,cb) {
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

DataTarget.prototype.aggregate = function(param,cb) {
  debug('DataTarget.prototype.aggregate');
  if (this.databaseType == "mongo") {
    aggregateMongoDB(param,cb);
    return;
  }
  if (this.databaseType == "postgres") {
    aggregatePostgresDB(param,cb);
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

DataTarget.prototype.find = function(query,options,cb) {
  debug('DataTarget.prototype.find');
  if (this.databaseType == 'mongo') {
   findMongoDB(query,options,cb);
  }
  if (this.databaseType == "postgres") {
    findPostgresDB (query,options,cb);
  }
}

module.exports = new DataTarget();


