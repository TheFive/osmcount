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
OSMData.prototype.import = postgresMapper.import;
OSMData.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
OSMData.prototype.export = postgresMapper.export;

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


OSMData.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into osmdata (data) VALUES($1)";
}

OSMData.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var reducedItem = {};

  // Only Store Data that is in the DataFilter
  for (var k in dataFilter) {
    if (typeof(item[k])!='undefined') {
      reducedItem[k]=item[k];
    }
  }
  var data = util.toHStore(reducedItem);
  return  [data];
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




OSMData.prototype.insertData = function(data,cb) {
  debug('OSMData.prototype.insertData');
  insertDataToPostgres(data,cb);
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
  findPostgresDB (query,cb);
}

module.exports = new OSMData();
