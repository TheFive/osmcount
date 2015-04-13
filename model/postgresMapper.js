var pg     = require('pg');
var debug  = require('debug')('postgresMapper');
var should = require('should');


var config = require('../configuration.js');

exports.invertMap = function invertMap(map) {
	debug('invertMap');
  map.invertKeys= {};
  for (k in map.keys) {
    var pgkey = map.keys[k].toLowerCase();
    should.not.exist(map.invertKeys[pgkey]);
    map.invertKeys[pgkey]=k;
  }
}

function createWhereClause(map,query) {
  debug('createWhereClause');
  var whereClause = ""
  for (var k in map.keys) {
    if (typeof(query[k])!='undefined'){
      var op = '=';
      if (map.regex[k] == true) op = '~';
      if (whereClause != '') whereClause += " and ";
      whereClause += map.keys[k]+' '+op+" '"+query[k]+"'";
    }
  }
  debug("generated where clause: %s",whereClause);
  if(whereClause != '') whereClause = "where "+whereClause;
  return whereClause;
}

exports.countPostgres = function count(map,query,cb) {
  debug('count');
  var whereClause = createWhereClause(map,query);
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var queryStr = "select count(*) from "+map.tableName+ ' '+whereClause;
    console.log(queryStr);
    client.query(queryStr,function(err,result) {
      should.not.exist(err);
      var count = result.rows[0].count;
      cb (null,count);
      pgdone();
      return;
    })
  })
}

function createFieldList(map) {
  debug('createFieldList');
  should.exist(map.invertKeys);
  var fieldList = '';
  for (var k in map.invertKeys) {
    if (fieldList !='') fieldList += ",";
    fieldList += k;
  } 
  debug('FieldList %s',fieldList);
  return fieldList;
}

exports.find = function find(map,query,options,cb) {
  debug('find');
  var whereClause = createWhereClause(map,query);
  var fieldList = createFieldList(map);
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var result = [];
    var query = client.query("select "+fieldList+" from "+map.tableName+ ' '+whereClause);
    query.on('row',function(row){
      var json = {};
      for (k in map.invertKeys) {
        json[map.invertKeys[k]] = row[k];
      }
      console.log("convertet JSON:");
      console.dir(json);
      console.log("Original Row");
      console.dir(row);
      result.push(json);
    })
    query.on('end',function(){
      cb(null,result);
      pgdone();
    })
    query.on('error',function(err){
      cb(err,null);
      pgdone();
    })
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
    client.query(this.createTableString,function(err) {
      debug('%s Table Created',this.tableName);
      cb(err);
    });
    pgdone();
  }.bind(this))
} 

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    var dropString = "DROP TABLE IF EXISTS "+this.tableName;
    client.query(dropString,function(err){
      debug("%s Table Dropped",this.tableName);
      cb(null);
    });
    pgdone();
  }.bind(this))  
}

exports.initialise = function initialise(dbType,callback) {
  debug('exports.initialise');
  config.initialise();

  if (typeof(dbType) != 'function') {
    this.databaseType = dbType;
  }
  else {
    this.databaseType = config.getDatabaseType();
    callback = dbType;
  }
  if (this.databaseType == 'postgres') {
    exports.invertMap(this.map);
  }
  if (callback) callback();
}



function countMongoDB(query,cb) {
  debug('countMongoDB');
  var db = config.getMongoDB();
  var collectionName = this.collectionName;

  // Fetch the collection test
  var collection = db.collection(collectionName);
  collection.count(query, cb);
}

exports.count = function(query,cb) {
  debug('exports.count');
  if (this.databaseType == "mongo") {
    countMongoDB(query,cb).bind(this);
  }
  if (this.databaseType == "postgres") {
    exports.countPostgres(this.map,query,cb);
  }
}
