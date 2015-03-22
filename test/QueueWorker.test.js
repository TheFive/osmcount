var should    = require('should');
var pg        = require('pg');
var async     = require('async');


var config      = require('../configuration.js');
var WorkerQueue = require('../model/WorkerQueue.js');
var QueueWorker = require('../QueueWorker.js');

describe('QueueWorker',function(){
  before(function(bddone) {
    config.initialisePostgresDB();
    async.series([
      WorkerQueue.dropTable,
      WorkerQueue.createTable
    ],function(err) {
      should.not.exist(err);
      bddone();
    });
  });
  after(function(bddone) {
    bddone();
  })
/*  it('should insert Values',function(bddone) {
    var valueList = [{id:1,measure:"test",type:"insert",status:"open",exectime: new Date()}];

    WorkerQueue.insertData(valueList,function(err,data) {
      should.not.exist(err);
      QueueWorker.doNextJob(function(err,data){
        should.not.exist(err);
        QueueWorker.count({id:1,status:"done"},function(err,data){
          should.not.exist(err);
          should(data).equal('1');
        })
      })
    });
  })*/
})