var should =require('should');
var wochenaufgabe = require('../wochenaufgabe.js');
var helper = require('./helper.js')

describe('Wochenaufgabe', function() {
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  describe('tagCounterPharmacy', function() {
    it('should count different OSM Objects', function() {
    var result = {};
    var osmData = [{tags:{fixme:1,wheelchair:1,"contact:phone":1,name:1,opening_hours:1}},
             {tags:{        wheelchair:1,phone:1,          name:1,               }},
             {tags:{        w         :1                                        }}];
    wochenaufgabe.tagCounterPharmacy(osmData,result);
    should(result).match({existing:{fixme:1},missing:{wheelchair:1,phone:1,name:1,opening_hours:2}});
    });
    it ('should handle empty Objects',function() {
      var result = {};
      var osmData = [];
      wochenaufgabe.tagCounterPharmacy(osmData,result);
      should(result).match({existing:{fixme:0},missing:{wheelchair:0,phone:0,name:0,opening_hours:0}});
    });
  });
  describe('tagCounterGuidePost', function() {
    it('should count different OSM Objects', function() {
    var result = {};
    var osmData = [{tags:{hiking:1,"note:destination":1,"inscription":1,name:1,omaterial:1}},
             {tags:{        bicycle:1,inscription:1,          name:1,               }},
             {tags:{        w         :1                                        }}];
    wochenaufgabe.tagCounterGuidePost(osmData,result);
    should(result).match({existing:{bicycle:1,description:0,hiking:1,inscription:2,material:0,name:2,'note:destination':1,operator:0,ref:0},missing:{}});
    });
    it ('should handle empty Objects',function() {
      var result = {};
      var osmData = [];
      wochenaufgabe.tagCounterGuidePost(osmData,result);
      should(result).match({existing:{bicycle:0,description:0,hiking:0,inscription:0,material:0,name:0,'note:destination':0,operator:0,ref:0},missing:{}});
    });
  });
  describe('tagCounterNoSubSelector', function() {
    it('should count different OSM Objects', function() {
    var result = {};
    var osmData = [{tags:{hiking:1,"note:destination":1,"inscription":1,name:1,omaterial:1}},
             {tags:{        bicycle:1,inscription:1,          name:1,               }},
             {tags:{        w         :1                                        }}];
    wochenaufgabe.tagCounterNoSubSelector(osmData,result);
    should(result).match({existing:{},missing:{}});
    });
    it ('should handle empty Objects',function() {
      var result = {};
      var osmData = [];
      wochenaufgabe.tagCounterGuidePost(osmData,result);
      should(result).match({existing:{},missing:{}});
    });
  });
  describe.skip('tagCounter2', function() {
    it('should count different OSM Objects and split them', function() {
      var osmData = [{tags:{fixme:1,wheelchair:1,"contact:phone":1,name:1,opening_hours:1},
                      osmArea:[{"key":"1"},{"key":"12"}]},
               {tags:{        wheelchair:1,phone:1,          name:1,               },
                osmArea:[{"key":"1"},{"key":"13"}]},
               {tags:{        w         :1                                        },
                osmArea:[{"key":"2"},{"key":"23"}]}];
      var keyList = ["23","13","12","21"];
      var defJson = {measure:"test",timestamp:"hello"};
      var result = wochenaufgabe.tagCounter2(osmData,keyList,"key",defJson);
      should(result.length).equal(4);
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"23",
         existing:{fixme:0},
         missing:{wheelchair:1,phone:1,name:1,opening_hours:1},
         count:1});
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"13",
         existing:{fixme:0},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:1},
         count:1});
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"12",
         existing:{fixme:1},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:0},
         count:1});
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"21",
         existing:{fixme:0},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:0},
         count:0});
    });
    it ('should handle empty Objects too',function() {
      var osmData = [];
      var keyList = ["23","13","12"];
      var defJson = {measure:"test",timestamp:"hello"};
      var result = wochenaufgabe.tagCounter2(osmData,keyList,"key",defJson);
      should(result.length).equal(3);
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"23",
         existing:{fixme:0},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:0},
         count:0});
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"13",
         existing:{fixme:0},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:0},
         count:0});
      should(result).containEql(
        {measure:"test",
         timestamp:"hello",
         schluessel:"12",
         existing:{fixme:0},
         missing:{wheelchair:0,phone:0,name:0,opening_hours:0},
         count:0});
    });
  });
});