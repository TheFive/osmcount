var should = require('should');
var assert = require('assert');
var nock   = require('nock');


var lod           =require('../model/LoadOverpassData.js');
var wochenaufgabe = require('../wochenaufgabe.js');
var helper = require('./helper.js');





describe('LoadOverpassData',function() {
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  describe('createQuery',function() {
    it ('should put an error for wrong Wochenaufgaben', function (bddone) {
      var referenceJob = {};
      referenceJob.measure = "NotExisting";
      referenceJob.exectime = new Date();
      var list = lod.createQuery(referenceJob);
      should.exist(list);
      should(list.length).equal(0);
      should(referenceJob.error).equal("Wochenaufgabe nicht definiert");
      bddone();
    });
    it ('should generate 2 queries', function (bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};

      var referenceJob = {};
      referenceJob.measure = "test";
      referenceJob.exectime = new Date();
      var list = lod.createQuery(referenceJob);
      should.exist(list);
      should(list.length).equal(2);
      should(list[0]).eql({measure:"test",
                                 exectime:referenceJob.exectime,
                                 status:"open",
                                 query:"TEST 1 TEST",
                                 schluessel:'1',
                                 type:'overpass',
                                 errorcount:0,
                                 overpasstime:0
                               });
      should(list[1]).eql({measure:"test",
                                 exectime:referenceJob.exectime,
                                 status:"open",
                                 query:"TEST 2 TEST",
                                 schluessel:'2',
                                 type:'overpass',
                                 errorcount:0,
                                 overpasstime:0
                               });

      bddone();
    });
    it('should work with key lists', function (bddone) {

      var keylist = {"DE":{name:"test"},
                      "DE1":{osmkey:"key",osmvalue:"1",name:"DE1"},
                      "DE2":{osmkey:"key",osmvalue:"2",name:"DE2"}}
      wochenaufgabe.map["test"]={map:{keyList:keylist},overpass:{query:"TEST :key: TEST :value:"}};

      var referenceJob = {};
      referenceJob.measure = "test";
      referenceJob.exectime = new Date();
      var list = lod.createQuery(referenceJob);
      should.exist(list);
      should(list.length).equal(2);
      should(list[0]).eql({measure:"test",
                                 exectime:referenceJob.exectime,
                                 status:"open",
                                 query:"TEST key TEST 1",
                                 schluessel:'DE1',
                                 type:'overpass'
                               });
      should(list[1]).eql({measure:"test",
                                 exectime:referenceJob.exectime,
                                 status:"open",
                                 query:"TEST key TEST 2",
                                 schluessel:'DE2',
                                 type:'overpass'
                               });

      bddone();
    });
    it('should return nothing', function (bddone) {

     
      wochenaufgabe.map["test"]={map:{},overpass:{query:"TEST :key: TEST :value:"}};

      var referenceJob = {};
      referenceJob.measure = "test";
      referenceJob.exectime = new Date();
      var list = lod.createQuery(referenceJob);
      should.exist(list);
      should(list.length).equal(0);
      bddone();
    });
  });
  describe('overpassQuery',function(bddone) {
    it('should handle a query',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .matchHeader('User-Agent', /osmcount\/.*/)
                  .post('',"data=This%20is%20an%20overpass%20query")
                
                  .reply(200, {
                    name: 'someJsonData',
                    data:[{name:3}]
                  });
      lod.overpassQuery("This is an overpass query",function(error,body) {
        should.not.exist(error);
        body = JSON.parse(body);
        should(body).eql({name: 'someJsonData',data:[{name:3}]});
        bddone();
      })
    })
    it('should handle an alternative url',function(bddone) {
      var scope = nock('http://alternative.overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpass%20query")
                
                  .reply(200, {
                    name: 'someJsonData',
                    data:[{name:3}]
                  });
      lod.overpassQuery("This is an overpass query",function(error,body) {
        should.not.exist(error);
        body = JSON.parse(body);
        should(body).eql({name: 'someJsonData',data:[{name:3}]});
        bddone();
      },{overpassUrl:'http://alternative.overpass-api.de/api/interpreter'})
    })
    it('should handle an Overcrowded',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpass%20query")
                
                  .reply(504, "Server Overcrowded");
      lod.overpassQuery("This is an overpass query",function(error,body) {
        should.exist(error);
        should(error.statusCode).equal(504);
        bddone();
      })
    })
    it('should handle a long running query',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpass%20query")
                  .socketDelay(5*1000)
                  
                  .reply(504, "Server Overcrowded");
      lod.overpassQuery("This is an overpass query",function(error,body) {
        should.exist(error);
        should(error.statusCode).equal(504);
        bddone();
      })
    })
    it('should handle a timeout',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpass%20query")
                  .socketDelay(5*1000)
                  
                  .reply(504, "Server Overcrowded");
      lod.overpassQuery("This is an overpass query",function(error,body) {
        should.exist(error);
        should(error.code).equal('ESOCKETTIMEDOUT');
        bddone();
      },{timeout:500})
    })
  });
  describe('runOverpass',function() {
    it ('should load and parse overpassdata',function(bddone){
      wochenaufgabe.map["test"]={map:{list:['1','2']},
                                 overpass:{query:"TEST :schluessel: TEST"},
                                 tagCounter:wochenaufgabe.tagCounterPharmacy};

      var job = {measure:"test"};
      job.exectime = new Date();
      var result = {};
      var scope = nock('http://overpass-api.de/api/interpreter')
                    .post('',"data=This%20is%20an%20overpass%20query")
                  
                    .replyWithFile(200, __dirname+"/LoadOverpassData.test.json");
      lod.runOverpass("This is an overpass query",job,result,function(error,body) {
        should.not.exist(error);
        should.exist(job.overpassTime);
        should(result.timestamp).equal(job.exectime);
        should(result.count).equal(12);
        should(result.missing).eql({ opening_hours: 1, phone: 1, wheelchair: 5, name: 0 });
        should(result.existing).eql({ fixme: 0 });
        bddone();
      });
    })
    it ('should work with alternative Urls',function(bddone){
      wochenaufgabe.map["test"]={map:{list:['1','2']},
                                 overpass:{query:"TEST :schluessel: TEST"},
                                 tagCounter:wochenaufgabe.tagCounterPharmacy,
                                 overpassUrl:'http://alternative.overpass-api.de/api/interpreter'};

      var job = {measure:"test"};
      job.exectime = new Date();
      var result = {};
      var scope = nock('http://alternative.overpass-api.de/api/interpreter')
                    .post('',"data=This%20is%20an%20overpass%20query")
                  
                    .replyWithFile(200, __dirname+"/LoadOverpassData.test.json");
      lod.runOverpass("This is an overpass query",job,result,function(error,body) {
        should.not.exist(error);
        should.exist(job.overpassTime);
        should(result.timestamp).equal(job.exectime);
        should(result.count).equal(12);
        should(result.missing).eql({ opening_hours: 1, phone: 1, wheelchair: 5, name: 0 });
        should(result.existing).eql({ fixme: 0 });
        bddone();
      });
    })
    it('should load and parse overpassdata Function 2',function(bddone){
      wochenaufgabe.map["test"]={map:{list:['1','2']},
                                 overpass:{query:"TEST :schluessel: TEST"},
                                 tagCounter:wochenaufgabe.tagCounterNoSubSelector};

      var job = {measure:"test"};
      job.exectime = new Date();
      var result = {};
      var scope = nock('http://overpass-api.de/api/interpreter')
                    .post('',"data=This%20is%20an%20overpass%20query")
                  
                    .replyWithFile(200, __dirname+"/LoadOverpassData.test.2.json");
      lod.runOverpass("This is an overpass query",job,result,function(error,body) {
        should.not.exist(error);
        should.exist(job.overpassTime);
        should(result.timestamp).equal(job.exectime);
        should(result.count).equal(116);
        should(result.missing).eql({  });
        should(result.existing).eql({  });
        bddone();
      });
    })
  })
});