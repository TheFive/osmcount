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
  'Int32Array',]

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

