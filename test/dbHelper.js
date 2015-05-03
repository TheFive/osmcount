var path = require('path');
var fs   = require("fs");
var async = require('async');
var should = require('should');
var debug = require('debug');


exports.initialiseTablePostgres = function initialiseTable(collection,importfile,cb) {
  debug('initialiseTablePostgres');

  // shift Parameter if importfile is not used
  var doImport = true;
  if (typeof importfile == 'function') {
    cb = importfile;
    doImport = false;
  }
  async.series([
    collection.dropTable.bind(collection),
    collection.createTable.bind(collection),
    function(callback) { 
      collection.initialise('postgres');
      if (doImport) {
        var filename = path.resolve(__dirname,importfile);
        collection.import(filename,callback);        
      } else callback();
    }
  ],function(err) {
    cb(err);
  });        
}

exports.dropCollection = function dropCollection(db, collectionName, cb) {
  should.exist(db,"Mongodb not initialised");
  var c = (db.collection(collectionName)); 
  if (!c) {
    cb();
    return;
  }
  c.drop(function(err,reply) {
    if (err) {
      // ignore this error, that collection does not exists
      if (err == "MongoError: ns not found") {
        cb();
        return;
      }
      console.log("Drop Error");
      err.occurs = "dropCollection";
      cb(err);
      return;
    }
    cb();
    return;
  })
}

exports.createCollection = function createCollection(db, collectionName, cb) {
  should.exist(db,"Mongodb not initialised");
  db.createCollection(collectionName,function(err,data) {
    if (err) {
      err.occurs = "createCollection";
      cb(err);
      return;
    }
    cb();
    return;
  })
}

exports.fillCollection = function fillCollection(db,collectionName,filename,cb) {
  filename = path.resolve(__dirname, filename);
  var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
  try {
    var data = JSON.parse(filestring);
  } catch(err) {
    if (err) {
      console.log("Error Parsing: "+filename);
      throw(err);
    }
  }
  should.exist(data);
  should.exist(db,"Mongodb not initialised");
  db.collection(collectionName).insert(data,{w:1}, function(err,data) {
    if (err) {
      err.occurs = "fillCollection";
      cb(err);
      return
    }
    cb();
    return;
  })
} 

exports.prepareCollection = function prepareCollection(db,collectionName,filename,cb) {
  async.series([
    function(cb) {exports.dropCollection(db,collectionName,cb);},
    function(cb) {exports.createCollection(db,collectionName,cb);},
    function(cb) {exports.fillCollection(db,collectionName,filename,cb);},
    ],
    function(err) {cb(err);});
}

