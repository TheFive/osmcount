var debug  = require('debug')('POI');
var pg     = require('pg');
var fs     = require('fs');
var async  = require('async');
var should = require('should');
var ProgressBar = require('progress');


var config    = require('../configuration.js');
var importCSV = require('../ImportCSV.js');
var util      = require('../util.js')
var postgresMapper = require('../model/postgresMapper.js')


function POI() {
  debug('POI');
  this.databaseType = "postgres"; 
  this.tableName = "POI";
  this.collectionName = "POI";
  this.createTableString = "CREATE TABLE poi ( id bigserial NOT NULL,data JSON, \
      CONSTRAINT id_poi PRIMARY KEY (id) )";
 this.map= {
    tableName:"poi",
    regex:{schluessel:false},
    keys: {id:'id'
         }}
}

POI.prototype.dropTable = postgresMapper.dropTable;
POI.prototype.createTable = postgresMapper.createTable;
POI.prototype.initialise = postgresMapper.initialise;
POI.prototype.count = postgresMapper.count;
POI.prototype.import = postgresMapper.import;
POI.prototype.insertData = postgresMapper.insertData;
POI.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
POI.prototype.export = postgresMapper.export;







POI.prototype.importCSV =function(filename,defJson,cb) {
  debug('POI.prototype.importCSV');
  console.log("No importCSV implemented for POI, try import JSON");
  cb("No importCSV implemented",null);
}


POI.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into poi (data) VALUES($1)";
}

POI.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var data = item;
  return  [data];
}






POI.prototype.insertData = function(data,cb) {
  debug('POI.prototype.insertData');
  if (this.databaseType == "postgres") {
    insertDataToPostgres(data,cb);
  }
}

function isBasisType(object) {
  var a= typeof(object);
  if (a=='string') return true;
  if (a=='number') return true;
  return false;
}

function generateWhereClause(query) {
  var whereClause ='';
  for (k in query) {
    if (whereClause != '') whereClause += " and ";
    if (isBasisType(query[k])) {
      whereClause += "data ->>'"+k + "' = '" +query[k]+"'";
    } else {
      for (k2 in query[k]) {
        should(isBasisType(query[k][k2])).equal(true);
        whereClause += "data ->'"+k +"'->>'"+k2 +"' = '" +query[k][k2]+"'";
      }      
    }
  }
  if (whereClause != '') whereClause = " where "+whereClause;
  return whereClause; 
}

POI.prototype.find = function(query,cb) {
  debug('POI.prototype.find');
  var whereClause = generateWhereClause(query);
  
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var queryStr = "select id,data from poi"+whereClause;
    var query = client.query(queryStr);
    var rows = [];
    query.on('row', function(row) {
      row.data._id = row.id;
      rows.push(row.data);
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

POI.prototype.remove = function(query,cb) {
  debug('POI.prototype.remove');
  debug('Query %s',JSON.stringify(query));
  var whereClause = generateWhereClause(query);


  
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var queryStr = "delete from poi"+whereClause;
    debug("Query String: %s",queryStr);
    var query = client.query(queryStr);
    var rows = [];
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

POI.prototype.save = function savePOI(poi,cb) {
  debug("save");
  should.exist(poi._id);
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var queryStr = "update poi set (data) = ($1) where id = $2";
    debug("Query String: %s",queryStr);
    var id = poi._id;
    delete poi._id;
    var query = client.query(queryStr,[poi,id]);
    poi._id = id;
    query.on('end', function(result) {
      //fired once and only once, after the last row has been returned and after all 'row' events are emitted
      //in this example, the 'rows' array now contains an ordered set of all the rows which we received from postgres
      cb(null,null);
    })  
    query.on('error', function(error) {
      //fired once and only once, after the last row has been returned and after all 'row' events are emitted
      //in this example, the 'rows' array now contains an ordered set of all the rows which we received from postgres
      cb(error);
    })  
  })

}


module.exports = new POI();
