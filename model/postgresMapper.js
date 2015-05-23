var pg     = require('pg');
var debug  = require('debug')('postgresMapper');
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
  for (var k in map.keys) {
    var pgkey = map.keys[k].toLowerCase();
    should.not.exist(map.invertKeys[pgkey]);
    map.invertKeys[pgkey]=k;
  }
}

function createWhereClause(map,query,options) {
  debug('createWhereClause');
  var whereClause = ""
  for (var k in map.keys) {
    if (typeof(query[k])!='undefined'){
      var op = '=';
      var value = query[k];
      if (value instanceof Date) {
        value = value.toISOString();
      }
      
      if (map.regex[k] == true) op = '~';
      if (whereClause != '') whereClause += " and ";
      whereClause += map.keys[k]+' '+op+" '"+value+"'";
    }
  }
  if (options) {
    if (typeof(options.sort)!='undefined' ) {
      whereClause += 'order by '+map.keys[options.sort];
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

exports.countUntilNowPostgres = function countUntilNow(map,query,cb) {
  debug('count');
  var whereClause = createWhereClause(map,query);
  var now = new Date();
  whereClause += " and exectime <= '"+now.toISOString() +"'";

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
  for (var i =0;i<map.hstore.length;i++) {
    var k = map.hstore[i];
    if (fieldList !='') fieldList += ",";
    fieldList += "hstore_to_json("+k+") as "+k;
  }
  debug('FieldList %s',fieldList);
  return fieldList;
}


exports.find = function find(query,options,cb) {
  // This function supports 2 kind of parameter sets
  // the old one, a mongo queryobject options and a callback
  // and the new one, a where clause and a callback
  debug('%s.find',this.tableName);

  var map = this.map;
  var whereClause = "";
  if (typeof query == 'string') {
    whereClause = query;
    cb = options;
  } else {
    if (typeof(options) == 'function') {
      cb = options;
      options = null;
    }
    whereClause = createWhereClause(map,query,options);
  }

  var fieldList = createFieldList(map);
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    var result = [];
    var queryStr = "select "+fieldList+" from "+map.tableName+ ' '+whereClause;
    var query = client.query(queryStr);
    query.on('row',function(row){
      var json = {};
      for (var k in map.invertKeys) {
        json[map.invertKeys[k]] = row[k];
      }
      for (var i = 0;i<map.hstore.length;i++) {
        var k = map.hstore[i];
        json[k]=row[k];
      }
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
  debug('%s.createTable',this.tableName);
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err);
      pgdone();
      return;
    }
    client.query(this.createTableString,function(err) {
      debug('%s Table Created',this.tableName);
      if (typeof(this.createIndexString)!='undefined') {
        client.query(this.createIndexString,function(err){
          debug('%s Index Created',this.tableName);
          cb(err);
          pgdone();
        })
      } else {
        // No Index to be defined, close Function correct
        cb(err);
        pgdone();
      }
    }.bind(this));
  }.bind(this))
} 

exports.dropTable = function(cb) {
  debug('%s.dropTable',this.tableName);
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
  debug('%s.initialise',this.tableName);
  config.initialise();

  if (typeof(dbType) != 'function') {
    should(dbType).equal("postgres");
  }
  else {
    callback = dbType;
  }
  exports.invertMap(this.map);
  if (typeof(this.map.hstore)=='undefined') {
    this.map.hstore = [];
  }
  if (callback) callback();
}


var getStream = function (filename) {
    var stream = fs.createReadStream(filename, {encoding: 'utf8'});
        return stream;
};




exports.import = function(filename,cb) {
  debug('%s.import',this.tableName);
  var me = this;

  async.auto(
    {checkFile:function (cb) {fs.exists(filename,function(result){var error;if (!result) error ="File Not Exist"; cb(error)});},
     import:["checkFile",function (cb) {  
      var stream = getStream(filename);;
      me.insertStreamToPostgres(stream,cb);}]},
      function (error,result) {cb(error,result.import);}
    );
  
}




exports.count = function(query,cb) {
  debug('%s.count',this.tableName);
  exports.countPostgres(this.map,query,cb);
}

exports.countUntilNow = function(query,cb) {                          
  debug('%s.countUntilNow',this.tableName);
  exports.countUntilNowPostgres(this.map,query,cb);
}

exports.insertStreamToPostgres = function insertStreamToPostgres(stream,cb) {
 
  // This Functions imports a stream or an array to postgres
  // depending of the type of the first argument
  // First detect type of first argumen

  // Dummy, used as a workaround, as stream was not declared in dd otherwise
  var streamx = stream;

  var array;
  var internal = false;
  var arrayUsed = false;

  // Check first parameter 
  // is it an array, so switch to array import.
  if (streamx instanceof Array) {
    array = streamx;
    streamx = null;
    internal = true;
    arrayUsed = true;
  }


  debug('insertStreamToPostgres');
  pg.connect(config.postgresConnectStr,function dd(err, client,pgdone) {
    debug('insertStreamToPostgres.1');
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

      //write some Date, so invalidate Cache before
      if (typeof(this.invalidateCache)=='function') {
        this.invalidateCache(item);
      }

      if (!internal) {
   
        if (typeof(item.collection) == 'string') {
          if (item.collection == this.tableName) {
            should(item.version).equal(1,"Import Version is not equal 1");
            versionDefined = true;
            bar = new ProgressBar('Importing '+this.tableName+': [:bar] :percent :current :total :etas', { total: item.count });
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
    if (arrayUsed) {
      debug("internal initialising");
      var streamx = es.readArray(array);

      ls = streamx.pipe(es.map(insertData.bind(this)));
    }
    else {
      debug(" not internal initialising");
      parser = JSONStream.parse();
      var mapper = es.map(insertData.bind(this));
      console.log(streamx);
      ls = stream.pipe(parser).pipe(mapper);
    }
    ls.wasCalled = false;

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
  debug('%s.insertData',this.tableName);

  // Turn Data into a stream
  //var reader = es.readArray(data);

  // use Stream Function to put data to Postgres
  this.insertStreamToPostgres(data,cb);
}

function exportCollection(callback,result)
{
  debug('export');
  should(false,"To be impleemented");
/*  var db = config.getMongoDB();
  var collection = db.collection(this.collectionName);
  collection.find({},function exportsMongoDBCB1(err,data) {
    debug('export->DB');
    if (err) {
      console.log("exportMongoDB Error: "+err);
      if (cb) cb(err);
      return;
    }
    var countCollection = result.count;
    var version = {"version":1,"collection":this.tableName,count:countCollection};
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
  }.bind(this))*/
}

// Exports all DataCollection Objects to a JSON File
exports.export = function(filename,cb){
  debug('%s.export',this.tableName);
  should(false,"not implemented yet");
}