var pg     = require('pg');
var fs     = require('fs');
var path   = require('path');
var async  = require('async');
var should = require('should');

var config      = require('../configuration.js');
var WorkerQueue = require('../model/WorkerQueue.js')


describe('WorkerQueue', function() {
  describe('import',function(bddone) {
    beforeEach(function(bddone) {
      async.series([
        WorkerQueue.dropTable,
        WorkerQueue.createTable
      ],function(err) {
        if (err) console.dir(err);
        should.equal(null,err);
        bddone();
      });
    });
    it('should import data',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.json");
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      WorkerQueue.import(filename,function(err,data){
        if (err) console.dir(err);
        should.equal(err,null);
        should(data).eql("Datensätze: 2");
        bddone();
      });
    });
    it('should handle wrong connection',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.json");
      var configTemp = config.postgresConnectStr;
      config.postgresConnectStr = "illegalConnectionString";
      WorkerQueue.import(filename,function(err,data){
        should.exist(err);
        config.postgresConnectStr = configTemp;
        bddone();
      });
    });
  });
})
