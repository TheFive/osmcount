var should  = require('should');
var numeral = require('numeral');
var util    = require('../util');

var config  = require('../configuration.js');

describe('util',function(){

  describe('copy', function(){
    
      it('clone should return equal objects', function(){
        var a = {a:1,b:2};
        should(a).eql(util.clone(a));
        var b = {};
        should(b).eql(util.clone(b));
        var c = {a:"Value",b:{c:"22",d:1},Z:function d(){return 1;}}
        should(c).eql(util.clone(c));
        var d = util.clone(c);
        should(c.Z()).eql(d.Z());
      });
      it('clone Objects should be "different" afterwards', function() {
        var a = {a:1,b:2};      
        var b = util.clone(a);
        a.a = 2;
        b.b = 5;
        should(a).eql({a:2,b:2});
        should(b).eql({a:1,b:5});
      })
   
  })

  describe('cloneObject',function() {
      it('copyObject should copy values without destroying destination',function() {
        var a = {k1:1,k2:2};
        var b = {k1:5,k3:"new Value"};
        util.copyObject(a,b);
        should(a).eql({k1:5,k2:2,k3:"new Value"});
      })
  })

  describe('waitOneMin', function() {
    this.timeout(1000*60*2+100);
    it('test Wait',function(done) {
      var t1 = new Date();
      util.waitOneMin(function test() {
        var t2 = new Date();
        should.equal(Math.round((t2-t1)/10),Math.round(config.getValue("waitTime",120)*1000)/10);
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
        should.equal(Math.round((t2-t1)/10),Math.round(2*1000)/10);
        done();
      })
    })
  })

  describe('waitOneMin second time', function() {
    this.timeout(1000*60*2+100);
    it('test Wait',function(done) {
      var t1 = new Date();
      util.waitOneMin(function test() {
        var t2 = new Date();
        should.equal(Math.round((t2-t1)/10),Math.round(config.getValue("waitTime",120)*1000)/10);
        done();
      })
    })
  })

  describe('initailise',function() {
    it('should set the default language to DE',function(){
      util.initialise();
      should(numeral(1.1).format("1.0")).equal('1,1');
      should(numeral(1000).format()).equal('1.000');
      should(numeral(1).format('0o')).equal('1.');
    })
  })
  describe('toHStore',function(){
    it('should generate an empty hStore',function(){
      should(util.toHStore({})).equal("");
    })
    it('should generate an one poperty hStore',function(){
      should(util.toHStore({name:"test"})).equal('"name"=>"test"');
      should(util.toHStore({name:1})).equal('"name"=>"1"');
    })
    it('should generate an one poperty hStore',function(){
      should(util.toHStore({name:"test"})).equal('"name"=>"test"');
      should(util.toHStore({name:1})).equal('"name"=>"1"');
    })
    it('should generate an more poperty hStore',function(){
      should(util.toHStore({name:"test",poing:"ping"})).equal('"name"=>"test","poing"=>"ping"');
      should(util.toHStore({name:"CountAll",count:2,missing:4})).equal('"name"=>"CountAll","count"=>"2","missing"=>"4"');
    })
    it('should escape " and \\ ',function () {
      should(util.toHStore({name:"test",poing:'ping "hallo"'})).equal('"name"=>"test","poing"=>"ping \\"hallo\\""');
      should(util.toHStore({name:"test",poing:'ping \\ me'})).equal('"name"=>"test","poing"=>"ping \\\\ me"');

    })
  })
})

