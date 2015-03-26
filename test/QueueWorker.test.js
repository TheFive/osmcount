var should    = require('should');
var pg        = require('pg');
var async     = require('async');


var config      = require('../configuration.js');
var util      = require('../util.js');
var WorkerQueue = require('../model/WorkerQueue.js');
var QueueWorker = require('../QueueWorker.js');
var wochenaufgabe = require('../wochenaufgabe.js');

describe('QueueWorker',function(){
  beforeEach(function(bddone) {
    config.initialisePostgresDB();
    async.series([
      WorkerQueue.dropTable,
      WorkerQueue.createTable
    ],function(err) {
      console.log('### closed beforeEach QueueWorker');
      should.not.exist(err);
      bddone();
    });
  });
  describe('doNextJob',function() {   
    it('should generate an Error with insert Values',function(bddone) {
      var valueList = [{id:1,measure:"notexistm",type:"insert",status:"open",exectime: new Date()}];

      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          WorkerQueue.count({status:"error"},function(err,data){
            should.not.exist(err);
            should(data).equal('1');
            bddone();
          })
        })
      });
    })
    it('should trigger insert Values',function(bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var valueList = [{id:1,measure:"test",type:"insert",status:"open",exectime: new Date()}];

      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function (done) {
              WorkerQueue.count({status:"open"},function(err,data){
                should.not.exist(err);
                should(data).equal('2');
                done();   
              })        
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
  })
  describe('runNextJobs',function(){
    it('should work 3 doConsole',function(bddone) {
      this.timeout(1000*60*2+100);
      var valueList = 
        [{id:1,measure:"test",type:"console",status:"open",exectime: new Date()},
         {id:2,measure:"test",type:"console",status:"open",exectime: new Date()},
         {id:3,measure:"test",type:"console",status:"open",exectime: new Date()}];
      var waiter = util.createWaiter(1);
      QueueWorker.processExit = function() {};
      WorkerQueue.insertData(valueList,function(err,data) {
        async.auto(
          {  runNextJobs:QueueWorker.startQueue,
             wait1sec: waiter,
             interrupt:["wait1sec",function(cb) {QueueWorker.processSignal="SIGINT";cb()}],
             test:["runNextJobs",function(cb){
                WorkerQueue.count({status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('3');
                cb(); 
             })}]
          },function (err) {
            should.not.exist(err);
            bddone();
          }
        )
      });
    });
  })
})

