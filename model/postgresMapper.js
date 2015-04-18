var pg     = require('pg');
var debug  = require('debug')('postgresMapper');
var should = require('should');
var fs = require('fs');

var JSONStream  = require('JSONStream');
var es          = require('event-stream');
var ProgressBar = require('progress');
var async       = require('async');
var should      = require('should');



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
      pgdone();
    });
  }.bind(this))
} 

exports.dropTable = function(cb) {
  debug('exports.dropTable');
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
     debug('exports.dropTable->connected');
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    var dropString = "DROP TABLE IF EXISTS "+this.tableName;
    var query = client.query(dropString);
    query.on('error',function(err){
      debug("%s Table Dropped",this.tableName);
      cb(err);
      pgdone();
    });
    query.on('end',function(){cb(null);pgdone();})
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


var getStream = function (filename) {
    var stream = fs.createReadStream(filename, {encoding: 'utf8'});
        return stream;
};




exports.import = function(filename,cb) {
  debug('exports.import')
  should(this.databaseType).equal('postgres');
  var me = this;

  async.auto(
    {checkFile:function (cb) {fs.exists(filename,function(result){var error;if (!result) error ="File Not Exist"; cb(error)});},
     import:["checkFile",function (cb) {  var stream = getStream(filename);;
      me.insertStreamToPostgres(false,stream,cb);}]},
      function (error,result) {cb(error,result.import);}
    );
  
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

exports.insertStreamToPostgres = function insertStreamToPostgres(internal,stream,cb) {
  debug('insertStreamToPostgres');
  pg.connect(config.postgresConnectStr,function(err, client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }

    var counter = 0;
    var bar;
    var versionDefined = false;


    function insertData(item,callback) {
      debug('insertStreamToPostgres->insertData');
      if (!internal) {
   
        if (typeof(item.collection) == 'string') {
          if (item.collection == this.tableName) {
            should(item.version).equal(1,"Import Version is not equal 1");
            versionDefined = true;
            bar = new ProgressBar('Importing "+this.tableName+": [:bar] :percent :current :total :etas', { total: item.count });
            callback();
            return;
          }
        }
        should.ok(versionDefined,"No Version Number and FileType in File");    
      }
      var valueList = this.getInsertQueryValueList(item);
      var queryString = this.getInsertQueryString();
      var query = client.query(this.getInsertQueryString(),valueList);
      query.on("error",function(err){
        debug('Error after Insert'+err);
        err.item = item;
        callback(err);
      });
      query.on("end",function(){        
        counter = counter +1;
        debug("query.end was called");
        if (bar) bar.tick();
        callback();
      });
      // Undocumneted Feature 
      // Create Backpressure to reduce write speed...
      return false;
    }

    var ls, mapper;
    var parser;
    if (internal) {
      ls = stream.pipe(es.map(insertData.bind(this)));
      parser = ls;
      mapper = ls;
    }
    else {
      parser = JSONStream.parse();
      var mapper = es.map(insertData.bind(this));
      ls = stream.pipe(parser).pipe(mapper);
    }
    ls.wasCalled = false;
    parser.on('end',function() {debug("parser.on('end');")})
    stream.on('end',function() {debug("stream.on('end');")})
    mapper.on('end',function() {debug("mapper.on('end');")})
    ls.on('end',function() {
      debug("ls.on('end')");

      // Quickhack  because is called two times
      if (!this.wasCalled) {
        var result = "Datensätze: "+counter;
        cb(null,result);
        pgdone();
      }
      this.wasCalled = true;
    })
    ls.on('error',function(err) {
      debug("ls.on('error);")

      cb(err);
    })
  }.bind(this))
}


exports.insertData = function insertData(data,cb) {
  debug('exports.insertData');
  if (this.databaseType == "mongo") {
    should.exist(null,"mongodb not implemented yet");
  }
  if (this.databaseType == "postgres") {

    // Turn Data into a stream
    var reader = es.readArray(data);

    // use Stream Function to put data to Postgres
    this.insertStreamToPostgres(true,reader,cb);
  }
}

function exportCollection(callback,result)
{
  debug('export');
  var db = config.getMongoDB();
  var collection = db.collection(this.collectionName);
  collection.find({},function exportsMongoDBCB1(err,data) {
    debug('export->DB');
    if (err) {
      console.log("exportMongoDB Error: "+err);
      if (cb) cb(err);
      return;
    }
    var countCollection = result.count;
    var version = {"version":1,"collection":this.collectionName,count:countCollection};
    var filename = result.filename;
    fs.writeFileSync(filename,JSON.stringify(version)+"\n");
    var bar = new ProgressBar('Exporting '+this.collectionName+': [:bar] :percent :current :total :etas', { total: countCollection });

    var count=0;
    data.each(function (err,doc) {
      if (err) {
        callback(err);
        return;
      }
      if (doc) {
        count++;
        bar.tick();
        delete doc.data;
        fs.appendFileSync(filename,JSON.stringify(doc)+'\n');
      } else {
        //console.log(filename +" is exported with "+count+" datasets.");
        callback();
        should(count).equal(countCollection);
      }
    })
  }.bind(this))
}

// Exports all DataCollection Objects to a JSON File
exports.export = function(filename,cb){
  debug('exports.export')
  should(this.databaseType).equal('mongo');
  var db = config.getMongoDB();
  should.exist(db,"MondoDB does not exist, not started ?");
  var collection = db.collection(this.collectionName);
  async.auto(
  {
    count: function(callback,result) {
      result.filename = filename;
      collection.count({},function(err,count){callback(err,count);})},

 /*   b : ["count",function(cb,result){console.log(result),cb()}],*/
    export: ["count",exportCollection.bind(this)]
  },function(err,result){cb(err);})
}