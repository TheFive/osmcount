var assert =require('assert');
var importCSV = require('../ImportCSV.js');
var configuration = require('../configuration.js');
var fs=require('fs');
var pg=require('pg');
var async=require('async');


describe('importCSV', function() {
  describe('parseCSV', function() {
    it ('should handle empty files', function() {
      var array = importCSV.parseCSV('',';');
      assert.notEqual(null,array);
      assert.equal(0,array.length);
    });
    it ('should handle header', function() {
      array = importCSV.parseCSV('name;count',';');
      assert.notEqual(null,array);
      assert.equal(1,array.length);
      assert.equal(array[0][0],'name');
      assert.equal(array[0][1],'count');
      array = importCSV.parseCSV('name;count\n',';');
      assert.equal(array[0][0],'name');
      assert.equal(array[0][1],'count');
      assert.equal(1,array.length);
    });
    it ('should handle several linefeeds', function() {
      var array = importCSV.parseCSV('name;count\na;1\nb;2',';');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
      var array = importCSV.parseCSV('name;count\r\na;1\r\nb;2',';');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
      var array = importCSV.parseCSV('name;count\ra;1\rb;2',';');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
    });
    it ('should handle empty closing line', function() {
      var array = importCSV.parseCSV('name;count\na;1\nb;2\n',';');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
    });
    it ('should work with not equal number of fields per line', function() {
      var array = importCSV.parseCSV('name;count;sometimes\na;1\nb;2;3\n',';');
      assert.equal(array[0].length,3);
      assert.equal(array[1].length,2);
      assert.equal(array[2].length,3);
      assert.equal(array[2][2],'3');
    });
    it ('should work with several delimiters', function() {
      var array = importCSV.parseCSV('name,count\na,1\nb,2',',');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
    });
    it ('should handle with string delimiters', function() {
      var array = importCSV.parseCSV('"name",count\n"a",1\n"b",2',',');
      assert.equal(array[0][0],'name');
      assert.equal(array[1][0],'a');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
    });
    it ('should handle with string delimiters and linefeed', function() {
      var array = importCSV.parseCSV('"name",count\n"a\nb",1\n"b",2',',');
      assert.equal(array[0][0],'name');
      assert.equal(array[1][0],'a\nb');
      assert.equal(array[1][1],'1');
      assert.equal(array[2][0],'b');
      assert.equal(array[2][1],'2');
      assert.equal(array.length,3);
    });
  });
  describe('convertArrToJSON', function() {
    it('should handle string types', function() {
      var array = importCSV.parseCSV('string;integer;real\nstring;1;1.0\nstring2;0;-1.4\n',';');
      var defJson = {string:'',integer:'',real:''};
      var ja = importCSV.convertArrToJSON(array, defJson);
      assert.deepEqual({string:'string',integer:'1',real:'1.0'},ja[0]);
      assert.deepEqual({string:'string2',integer:'0',real:'-1.4'},ja[1]);
    });
    it('should handle integer types', function() {
      var array = importCSV.parseCSV('a;b;c\n1;10000;-15',';');
      var defJson = {a:0,b:0,c:0}
      var ja = importCSV.convertArrToJSON(array, defJson);
      assert.deepEqual({a:1,b:10000,c:-15},ja[0]);
    });
    it('should handle float types', function() {
      var array = importCSV.parseCSV('a;b;c\n1.2;10000.1;-15.2',';');
      var defJson = {a:0,b:0,c:0}
      var ja = importCSV.convertArrToJSON(array, defJson);
      assert.deepEqual({a:1.2,b:10000.1,c:-15.2},ja[0]);
    });
    it('should handle date types', function() {
      var array = importCSV.parseCSV('a;b;c\n2014-10-12;2000-01-01;0000-00-00',';');
      var defJson = {a:new Date(),b:new Date(),c:new Date()}
      var ja = importCSV.convertArrToJSON(array, defJson);
      assert.deepEqual(new Date(2014,10-1,12,0,0,0),ja[0].a);
      assert.deepEqual(new Date(2000,1-1,1,0,0,0),ja[0].b);
      assert.deepEqual(new Date(0,0-1,0,0,0,0),ja[0].c);
    });
    it('should handle defaults', function() {
     var array = importCSV.parseCSV('string;integer;real\nstring;1;1.0\nstring2;0;-1.4\n',';');
      var defJson = {defaultValue:"default"}
      var ja = importCSV.convertArrToJSON(array, defJson);
      assert.deepEqual("default",ja[0].defaultValue);
    });
  });

  describe('readCSVMongoDB',function() {
    var db;
    before(function(done) {
      configuration.initialiseMongoDB( function () {
        db = configuration.getMongoDB();
        var c = (db.collection("DataCollection"));
        if (c) {
          c.drop(function(err,cb) {
          db.createCollection("DataCollection",function(err,data) {
            done();
          });
          });
        } else done();
      });
    });
    it('should fail with no filename' , function(done) {
      db = configuration.geMongotDB();
      var a = importCSV.readCSVMongoDB('NonExistingFile.csv',db,{},function(err,data) {
         assert.equal(err.errno,34);
         done();
      })
    });
    it('should load 2 datasets' , function(done) {
      db = configuration.getMongoDB();
      fs.writeFileSync("existingFile.csv","name;count\na;2\nb;10");
      var a = importCSV.readCSVMongoDB('existingFile.csv',db,{name:"",count:0},function(err,data) {
        assert.equal(err,null);
        assert.equal(data,"Datensätze: 2");
        db.collection("DataCollection").find({}).toArray(function(err,data) {
           assert.equal(data.length,2);
           assert.equal(data[0].name,'a');
           assert.equal(data[0].count,'2');
           assert.equal(data[1].name,'b');
          assert.equal(data[1].count,'10');
        })
        done();
      })
    });
    it('should handle empty Files' ,function (done) {
      fs.writeFileSync("emtyFile.csv","");
      var a = importCSV.readCSVMongoDB('emtyFile.csv',db,{name:"",count:0},function(err,data) {
        assert.equal(err,"empty file");
        done();
      });
    })

  });
  describe('readCSVpostgres',function() {
    var db;
    before(function(bddone) {
      configuration.initialisePostgresDB();
      pg.connect(configuration.postgresConnectStr, function(err,client,pgdone){
        assert.equal(err,null);
        async.series([
          function(done) {client.query("DROP TABLE IF EXISTS DataCollection",done);},
          function(done) {client.query("CREATE TABLE IF NOT EXISTS DataCollection (id SERIAL, data JSON);",done);},
        ],function(err) {
          assert.equal(null,err);
          pgdone();
          bddone();
        });
      });
    });
    it('should fail with no filename' , function(done) {
      var a = importCSV.readCSVPostgresDB('NonExistingFile.csv',{},function(err,data) {
        assert.equal(err.errno,34);
        done();
      })
    });
    it('should load 2 datasets' , function(done) {
      fs.writeFileSync("existingFile.csv","name;count\na;2\nb;10");
      var a = importCSV.readCSVPostgresDB('existingFile.csv',{name:"",count:0},function(err,data) {
        assert.equal(data,"Datensätze: 2");
        assert.equal(err,null);
        pg.connect(configuration.postgresConnectStr, function(err,client,pgdone){
          assert.equal(err,null);
          client.query("select data from DataCollection;",function(err,data){
            assert.equal(err,null);
            assert.notEqual(null,data);
            assert.equal(data.rows.length,2);
            assert.equal(data.rows[0].data.name,'a');
            assert.equal(data.rows[0].data.count,'2');
            assert.equal(data.rows[1].data.name,'b');
            assert.equal(data.rows[1].data.count,'10');
            done();
            pgdone();
            client.end();
            });
          });
      })
    });
    it('should handle empty Files' ,function (done) {
      fs.writeFileSync("emtyFile.csv","");
      var a = importCSV.readCSVPostgresDB('emtyFile.csv',{name:"",count:0},function(err,result) {
        assert.equal(err,"empty file");
        done();
      })
    })
  });
});
