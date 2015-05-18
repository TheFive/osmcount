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



var storedGlobals = [];
exports.storeGlobals = function storeGlobals() {
  storedGlobals = [];
  for (var k in GLOBAL) {
    storedGlobals.push(k)
  }
  should(storedGlobals).not.eql([]);
}

var globalAllowed = [
  'ArrayBuffer',
  'Int8Array',
  'DataView',
  'DTRACE_NET_SERVER_CONNECTION',
  'DTRACE_NET_STREAM_END',
  'DTRACE_NET_SOCKET_READ',
  'DTRACE_NET_SOCKET_WRITE',
  'DTRACE_HTTP_SERVER_REQUEST',
  'DTRACE_HTTP_SERVER_RESPONSE',
  'DTRACE_HTTP_CLIENT_REQUEST',
  'DTRACE_HTTP_CLIENT_RESPONSE',
  'global',
  'Uint8Array',
  'process',
  'GLOBAL',
  'root',
  'Buffer',
  'setTimeout',
  'setInterval',
  'clearTimeout',
  'clearInterval',
  'setImmediate',
  'clearImmediate',
  'Uint8ClampedArray',
  'console',
  'Reporter',
  'before',
  'after',
  'beforeEach',
  'afterEach',
  'run',
  'context',
  'describe',
  'xcontext',
  'Int16Array',
  'xdescribe',
  'specify',
  'it',
  'xspecify',
  'xit' ,'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float32Array',
  'Float64Array',
  'list',
  'i',
  'query',
  'result',
  'job',
  'c',
  'param',
  'value',
  'date',
  'k2',
  'doc',
  'key',
  'measure',
  'array',
  'Uint32Array']

exports.allowedGlobals = function allowedGlobals(allowed) {
  var unallowed = [];
  for (var k in GLOBAL) {
    if (storedGlobals.indexOf(k)>=0) {
      if (allowed.indexOf(k)<0 && globalAllowed.indexOf(k)<0) {
        unallowed.push(k);
      }
    }
  }
  should(unallowed).eql([]);
}



exports.initUnallowedGlobals = function() {
  query = "not used";
  i = "not used";
  result = "not used";
  list = "not used";
  job = "not used";
  c = "not used";
  param = "not used";
  value = "not used";
  date = "not used";
  k2 = "not used";
  doc = "not used";
  key = "not used";
  measure = "not used";
  array = "not used";
  exports.storeGlobals();
}

exports.checkUnallowedGlobals = function() {
  should(query).equal("not used","global var query is used");
  should(i).equal("not used","global var i is used");
  should(result).equal("not used","global var result is used");
  should(list).equal("not used","global var list is used");
  should(job).equal("not used", "global var job is used");
  should(c).equal("not used","global var c is used");
  should(param).equal("not used","global var param is used");
  should(value).equal("not used", "global var value is used");
  should(date).equal("not used", "global var date is used");
  should(k2).equal("not used", "global var k2 is used");
  should(doc).equal("not used", "global var doc is used");
  should(key).equal("not used", "global var key is used");
  should(measure).equal("not used", "global var measure is used");
  should(array).equal("not used", "global var array is used");
  //exports.allowedGlobals([]);
}

