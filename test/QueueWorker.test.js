var should    = require('should');
var pg        = require('pg');
var async     = require('async');
var nock      = require('nock');


var config         = require('../configuration.js');
var util           = require('../util.js');
var WorkerQueue    = require('../model/WorkerQueue.js');
var DataCollection = require('../model/DataCollection.js');
var QueueWorker    = require('../QueueWorker.js');
var wochenaufgabe  = require('../wochenaufgabe.js');
var LoadOverpassData  = require('../model/LoadOverpassData.js');

// Define some global variables to test, wether they are used
// or not. 
query = "not used";
result =  "not used";
var helper = require('./helper.js');

describe('QueueWorker',function(){
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  beforeEach(function(bddone) {
    async.series([
      WorkerQueue.dropTable.bind(WorkerQueue),
      WorkerQueue.createTable.bind(WorkerQueue),
      DataCollection.dropTable.bind(DataCollection),
      DataCollection.createTable.bind(DataCollection)
    ],function(err) {
      should.not.exist(err);
    //  helper.storeGlobals();
      bddone();
    });
  });
  afterEach(function(bddone){
    should(query).equal("not used");
    should(result).equal("not used");
   // helper.allowedGlobals([]);
    bddone();
  })
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
    it('should trigger insert Values and create new insert',function(bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"},overpassEveryDays:7};
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
                should(data).equal('3');
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
    it('should trigger insert Values without new insert',function(bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"},overpassEveryDays:7};
      var valueList = [{id:1,measure:"test",type:"insert",status:"open",exectime: new Date()},
                       {id:2,measure:"test",type:"insert",status:"open",exectime: new Date("2999-02-02")}
      ];

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
                should(data).equal('3');
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
    it('should work on overpass Queries',function(bddone) {
      this.timeout(1000*60*2+100);
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery")             
                  .replyWithFile(200, __dirname + '/LoadOverpassData.test.json');
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "done";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work handle not parsable results',function(bddone) {
      this.timeout(1000*60*2+100);
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery")             
                  .reply(200, "Not parsable JSON");
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "error";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"error"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('0');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work handle a timeout',function(bddone) {
      this.timeout(1000*60*2+100);
      var remindTimeout = LoadOverpassData.timeout;
      LoadOverpassData.timeout = 500;

      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery") 
                  .socketDelay(2000)            
                  .replyWithFile(200, __dirname + '/LoadOverpassData.test.json');
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "error";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"error"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('0');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              LoadOverpassData.timeout = remindTimeout;
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
      QueueWorker.processExit = function() {console.log('Simulated Process Exit');};
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

