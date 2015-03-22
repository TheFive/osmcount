var should = require('should');


var lod    =require('../LoadoverpassData.js');
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
});