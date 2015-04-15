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

DataTargetClass.prototype.dropTable = postgresMapper.dropTable;
DataTargetClass.prototype.createTable = postgresMapper.createTable;
DataTargetClass.prototype.initialise = postgresMapper.initialise;
DataTargetClass.prototype.count = postgresMapper.count;
DataTargetClass.prototype.import = postgresMapper.import;




DataTargetClass.prototype.importCSV =function(filename,defJson,cb) {
  debug('DataTarget.prototype.importCSV');
  console.log("No importCSV implemented for datatarget, try import JSON");
  cb("No importCSV implemented",null);
}


DataTargetClass.prototype.insertStreamToPostgres = function insertStreamToPostgres(internal,stream,cb) {
  debug('insertStreamToPostgres');
  debug('Connect String:'+config.postgresConnectStr);
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      cb(err);
      return;
    }

    var counter = 0;
    var bar;
    var versionDefined = false;

    return function insertData(item,callback){
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
   };
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



// Exports all DataCollection Objects to a JSON File
DataTargetClass.prototype.export = function(filename,cb){
  debug('DataTarget.prototype.export')
  should(this.databaseType).equal('mongo');
  exportMongoDB(filename,cb);
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

DataTargetClass.prototype.find = function(query,options,cb) {
  debug('DataTarget.prototype.find');
  if (this.databaseType == 'mongo') {
   findMongoDB(query,options,cb);
  }
  if (this.databaseType == "postgres") {
    findPostgresDB (query,options,cb);
  }
}

module.exports = new DataTargetClass();


