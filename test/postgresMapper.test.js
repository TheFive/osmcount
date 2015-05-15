var should = require('should');
var async = require('async')

var postgresMapper = require('../model/postgresMapper.js');
var util = require('../util.js');
var helper = require ('./helper.js');

function ClassAClass() {
  this.databaseType = "postgres"; 
  this.tableName = "ClassA";
  this.collectionName = "ClassA";
  this.createTableString = 'CREATE TABLE ClassA \
              (field_1 text, \
               field_2 text, \
               field_hs hstore) \
                WITH ( OIDS=FALSE ); ';
                            
  this.map={tableName:'ClassA',
         regex:{},
         hstore:["field_hs"],
         keys:{
          field_1:'field_1',
          field_2:'field_2',
         }}
}
ClassAClass.prototype.createTable = postgresMapper.createTable;
ClassAClass.prototype.dropTable = postgresMapper.dropTable;
ClassAClass.prototype.insertData = postgresMapper.insertData;
ClassAClass.prototype.insertStreamToPostgres = postgresMapper.insertStreamToPostgres;
ClassAClass.prototype.find = postgresMapper.find;
ClassAClass.prototype.initialise = postgresMapper.initialise;

ClassAClass.prototype.getInsertQueryString = function getInsertQueryString() {
  return "INSERT into classa (field_1,field_2,field_hs) VALUES($1,$2,$3)";
}

ClassAClass.prototype.getInsertQueryValueList = function getInsertQueryValueList(item) {  
  var field_1 = item.field_1;
  var field_2 = item.field_2;
  var field_hs  = util.toHStore(item.field_hs)
  return  [field_1,field_2,field_hs];
}
var classA = new ClassAClass();



describe('postgresMapper',function(){
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  beforeEach(function(bddone) {
    //helper.storeGlobals();
    bddone();
  })
  afterEach(function(bddone) {
    //helper.allowedGlobals([]);
    bddone();
  })
  describe('invoke methods with classA', function () {
    before(function(bddone) {  
      async.series([

        function(done) {classA.initialise("postgres",done)},
        function(done) {classA.dropTable(done)},
        function(done) {classA.createTable(done)}
      ],function(err) {
        if (err) console.dir(err);
        should.not.exist(err);
        bddone();
      });
    });
    it('should insert an object', function(bddone) {
      var testData = {field_1:"1",field_2:"2",field_hs:{f1:"1",f2:"2"}};
      var result;
      async.series([
        function(done) {classA.insertData([testData],done);},
        function(done) {classA.find({},function(err,r){result = r,done(err)})}
      ],function(err) {
        should.not.exist(err);
        should(result.length).equal(1);
        should(testData).eql(result[0]);
        bddone();
      })
      
    })

  })
  describe('invertMap',function() {
    it('should invert a map',function(bddone){
      var map = {keys:{a:'b',b:'c'}};
      postgresMapper.invertMap(map);
      should.exist(map.invertKeys);
      should(map.invertKeys).eql({b:'a',c:'b'});
      bddone();
    })
    it('should invert a map and lowercase all',function(bddone){
      var map = {keys:{a:'B',b:'C'}};
      postgresMapper.invertMap(map);
      should.exist(map.invertKeys);
      should(map.invertKeys).eql({b:'a',c:'b'});
      bddone();
    })
    it('should fail if not bijective',function(bddone){
      var map = {keys:{a:'b',b:'b'}};
      (function() {postgresMapper.invertMap(map)}).should.throw(Error);
      bddone();
    })
    it('should fail if not bijective in lowerCase Situations',function(bddone){
      var map = {keys:{a:'B',b:'b'}};
      (function() {postgresMapper.invertMap(map)}).should.throw(Error);
      bddone();
    })
  })
})