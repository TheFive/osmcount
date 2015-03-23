var should = require('should');
var assert = require('assert');
var nock   = require('nock');


var lod    =require('../LoadOverpassData.js');
var wochenaufgabe = require('../wochenaufgabe.js');



describe('LoadOverpassData',function() {
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
                                 type:'overpass'
                               });
      should(list[1]).eql({measure:"test",
                                 exectime:referenceJob.exectime,
                                 status:"open",
                                 query:"TEST 2 TEST",
                                 schluessel:'2',
                                 type:'overpass'
                               });

      bddone();
    });
  });
  describe('overpassQuery',function(bddone) {
    it('should handle a query',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20a%20overpassquery")
                
                  .reply(200, {
                    name: 'someJsonData',
                    data:[{name:3}]
                  });
      lod.overpassQuery("This is a overpassquery",function(error,body) {
        should.not.exist(error);
        body = JSON.parse(body);
        should(body).eql({name: 'someJsonData',data:[{name:3}]});
        bddone();
      })
    })
    it('should handle a Timeout',function(bddone) {
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20a%20overpassquery")
                
                  .reply(504, "Server Overcrowded");
      lod.overpassQuery("This is a overpassquery",function(error,body) {
        should.exist(error);
        should(error.statusCode).equal(504);
        bddone();
      })
    })
  });
});