var pg     = require('pg');
var fs     = require('fs');
var path   = require('path');
var async  = require('async');
var should = require('should');

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
  });
})
