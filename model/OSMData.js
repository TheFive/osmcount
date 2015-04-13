var debug  = require('debug')('OSMData');
var pg     = require('pg');
var fs     = require('fs');
var async  = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var util      = require('../util.js')
var postgresMapper = require('../model/postgresMapper.js')


function OSMData() {
  debug('OSMData');
  this.databaseType = "postgres"; 
  this.tableName = "OSMData";
  this.collectionName = "OSMBoundaries";
  this.createTableString = "CREATE TABLE osmdata ( id bigserial NOT NULL,data hstore, \
      CONSTRAINT id_osmdata PRIMARY KEY (id) )";
 this.map= {
    tableName:"osmdata",
    regex:{schluessel:false},
    keys: {id:'id'
         }}
}

OSMData.prototype.dropTable = postgresMapper.dropTable;
OSMData.prototype.createTable = postgresMapper.createTable;
OSMData.prototype.initialise = postgresMapper.initialise;
OSMData.prototype.count = postgresMapper.count;

var dataFilter =
{
  "name":1,
  "de:regionalschluessel":1,
  "postal_code":1,
  "ref:at:gkz":1,
  "de:amtlicher_gemeindeschluessel":1,
  boundary:1,
  admin_level:1,
  "de:regionalschluessel":1,
  "osmcount_country":1
}






OSMData.prototype.importCSV =function(filename,defJson,cb) {
  debug('OSMData.prototype.importCSV');
  console.log("No importCSV implemented for osmdata, try import JSON");
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
    var bar;
    if (data.length>100) {
      bar = new ProgressBar('Importing OSMData: [:bar] :percent :total :etas', { total: data.length });
    }
    function insertData(item,callback){
      debug('insertDataToPostgres->insertData');
      // only store data in Database from dataFilter
      var reducedItem = {};
      for (var k in dataFilter) {
        reducedItem[k]=item[k];
      }

      var data = util.toHStore(reducedItem);
      client.query("INSERT into osmdata (data) VALUES($1)",
                          [data], function(err,result) {
        if (err) {
          err.item = item;
          err.data = data;
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
  var collection = db.collection('OSMBoundaries');
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

OSMData.prototype.import = function(filename,cb) {
  debug('OSMData.prototype.import')
  should(this.databaseType).equal('postgres');
  importPostgresDB(filename,cb);
}

// Exports all DataCollection Objects to a JSON File
OSMData.prototype.export = function(filename,cb){
  debug('OSMData.prototype.export')
  should(this.databaseType).equal('mongo');
  exportMongoDB(filename,cb);
}

OSMData.prototype.insertData = function(data,cb) {
  debug('OSMData.prototype.insertData');
  if (this.databaseType == "mongo") {
    assert.equal("mongodb not implemented yet",null);
  }
  if (this.databaseType == "postgres") {
    insertDataToPostgres(data,cb);
  }
}

function findMongoDB(query,cb) {
  debug('findMongoDB');
  var db = config.getMongoDB();
  var collectionName = 'OSMBoundaries';

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.find(query).toArray(cb);
}

function findPostgresDB(query,cb) {
  debug('findPostgresDB');
  should(query).eql({});

  
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var query = client.query("select to_json(data) from osmdata");
    var rows = [];
    query.on('row', function(row) {
      rows.push(row.to_json);
    });
    query.on('end', function(result) {
      //fired once and only once, after the last row has been returned and after all 'row' events are emitted
      //in this example, the 'rows' array now contains an ordered set of all the rows which we received from postgres
      cb(null,rows);
    })  
    query.on('error', function(error) {
      //fired once and only once, after the last row has been returned and after all 'row' events are emitted
      //in this example, the 'rows' array now contains an ordered set of all the rows which we received from postgres
      cb(error);
    })  
  })

}

OSMData.prototype.find = function(query,cb) {
  debug('OSMData.prototype.find');
  if (this.databaseType == 'mongo') {
   findMongoDB(query,cb);
  }
  if (this.databaseType == "postgres") {
    findPostgresDB (query,cb);
  }
}

module.exports = new OSMData();
