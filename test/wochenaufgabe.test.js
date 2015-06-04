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
  describe('tagCounter', function() {
    it('should count different OSM Objects', function() {
    var result = {};
    var osmData = [{tags:{fixme:1,wheelchair:1,"contact:phone":1,name:1,opening_hours:1}},
             {tags:{        wheelchair:1,phone:1,          name:1,               }},
             {tags:{        w         :1                                        }}];
    wochenaufgabe.tagCounter(osmData,result);
    should(result).match({existing:{fixme:1},missing:{wheelchair:1,phone:1,name:1,opening_hours:2}});
    });
    it ('should handle empty Objects',function() {
      var result = {};
      var osmData = [];
      wochenaufgabe.tagCounter(osmData,result);
      should(result).match({existing:{fixme:0},missing:{wheelchair:0,phone:0,name:0,opening_hours:0}});
    });
  });
  describe('tagCounter2', function() {
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