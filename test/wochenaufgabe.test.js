var should =require('should');
var wochenaufgabe = require('../wochenaufgabe.js');

describe('Wochenaufgabe', function() {
  describe('tagCounter', function() {
    it('should count different OSM Objects', function() {
		var result = {};
		var osmData = [{tags:{fixme:1,wheelchair:1,"contact:phone":1,name:1,opening_hours:1}},
					   {tags:{        wheelchair:1,phone:1,          name:1,               }},
					   {tags:{        w         :1                                        }}];
		wochenaufgabe.tagCounter(osmData,result);
		should(result).match({existing:{fixme:1},missing:{wheelchair:1,phone:1,name:1,opening_hours:2}});
    });
    if ('should handle empty Objects',function() {
		var result = {};
		var osmData = [];
		wochenaufgabe.tagCounter(osmData,result);
		should(result).match({existing:{fixme:0},missing:{wheelchair:0,phone:0,name:0,opening_hours:0}});
    });
  });
});