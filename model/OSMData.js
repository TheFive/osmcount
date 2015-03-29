var debug  = require('debug')('OSMData');
var pg     = require('pg');
var fs     = require('fs');
var async  = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var util      = require('../util.js')

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

var databaseType = "postgres";

var postgresDB = {
  createTableString:
  "CREATE TABLE osmdata ( id bigserial NOT NULL,data hstore, \
      CONSTRAINT id_osmdata PRIMARY KEY (id) )"
};

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    client.query("DROP TABLE IF EXISTS osmdata",function(err){
      debug("OSMData Table Dropped");
      cb(err)
    });

    pgdone();
  })
}

exports.createTable = function(cb) {
  debug('exports.createTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    client.query(postgresDB.createTableString,function(err) {
      debug('OSMDat Table Created');
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

exports.find = function(query,cb) {
  debug('find');
  if (databaseType == 'mongo') {
   findMongoDB(query,cb);
  }
  if (databaseType == "postgres") {
    findPostgresDB (query,cb);
  }
}
