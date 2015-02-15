var assert = require("assert")
var util = require('../util');
var configuration = require('../configuration');


describe('copy', function(){
  
    it('clone should return equal objects', function(){
      var a = {a:1,b:2};
      assert.deepEqual(a,util.clone(a));
      var b = {};
      assert.deepEqual(b,util.clone(b));
      var c = {a:"Value",b:{c:"22",d:1},Z:function d(){return 1;}}
      assert.deepEqual(c,util.clone(c));
      var d = util.clone(c);
      assert.equal(c.Z(),d.Z());
    });
    it('clone Objects should be "different" afterwards', function() {
      var a = {a:1,b:2};      
      var b = util.clone(a);
      a.a = 2;
      b.b = 5;
      assert.deepEqual(a,{a:2,b:2});
      assert.deepEqual(b,{a:1,b:5});
    })
 
})

describe('cloneObject',function() {
    it('copyObject should copy values without destroying destination',function() {
      var a = {k1:1,k2:2};
      var b = {k1:5,k3:"new Value"};
      util.copyObject(a,b);
      assert.deepEqual({k1:5,k2:2,k3:"new Value"},a);
    })
})

describe('waitOneMin', function() {
	this.timeout(1000*60*2+100);
	it('test Wait',function(done) {
		var t1 = new Date();
		util.waitOneMin(function test() {
			var t2 = new Date();
			assert.equal(Math.round((t2-t1)/10),Math.round(configuration.getValue("waitTime",120)*1000)/10);
			done();
		})
	})
})

describe('createWaiter', function() {
	this.timeout(15000);
	it('wait for 2 seconds',function(done) {
		var a = util.createWaiter(2);
		var t1 = new Date();
		a(function test() {
			var t2 = new Date();
			assert.equal(Math.round((t2-t1)/10),Math.round(2*1000)/10);
			done();
		})
	})
})

describe('waitOneMin', function() {
	this.timeout(1000*60*2+100);
	it('test Wait',function(done) {
		var t1 = new Date();
		util.waitOneMin(function test() {
			var t2 = new Date();
			assert.equal(Math.round((t2-t1)/10),Math.round(configuration.getValue("waitTime",120)*1000)/10);
			done();
		})
	})
})

