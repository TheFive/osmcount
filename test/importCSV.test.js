var should= require('should');
var fs    = require('fs');
var pg    = require('pg');
var async = require('async');

var importCSV = require('../ImportCSV.js');
var configuration = require('../configuration.js');
var DataCollection=require('../model/DataCollection.js')


describe('importCSV', function() {
  describe('parseCSV', function() {
    it ('should handle empty files', function() {
      var array = importCSV.parseCSV('',';');
      should.exist(array);
      should.equal(0,array.length);
    });
    it ('should handle header', function() {
      array = importCSV.parseCSV('name;count',';');
      should.exist(array);
      should.equal(1,array.length);
      should(array[0]).match(['name','count']);

      array = importCSV.parseCSV('name;count\n',';');
      should.exist(array);
      should.equal(1,array.length);
      should(array[0]).match(['name','count']);
    });
    it ('should handle several linefeeds', function() {
      var array = importCSV.parseCSV('name;count\na;1\nb;2',';');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);

      var array = importCSV.parseCSV('name;count\r\na;1\r\nb;2',';');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);

      var array = importCSV.parseCSV('name;count\ra;1\rb;2',';');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);
    });
    it ('should handle empty closing line', function() {
      var array = importCSV.parseCSV('name;count\na;1\nb;2\n',';');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);
    });
    it ('should work with not equal number of fields per line', function() {
      var array = importCSV.parseCSV('name;count;sometimes\na;1\nb;2;3\n',';');
      should(array).match([['name','count','sometimes'],['a','1'],['b','2','3']]);
    });
    it ('should work with several delimiters', function() {
      var array = importCSV.parseCSV('name,count\na,1\nb,2',',');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);
    });
    it ('should handle with string delimiters', function() {
      var array = importCSV.parseCSV('"name",count\n"a",1\n"b",2',',');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a','1'],['b','2']]);
    });
    it ('should handle with string delimiters and linefeed', function() {
      var array = importCSV.parseCSV('"name",count\n"a\nb",1\n"b",2',',');
      should.equal(array.length,3);
      should(array).match([['name','count'],['a\nb','1'],['b','2']]);
    });
  });
  describe('convertArrToJSON', function() {
    it('should handle string types', function() {
      var array = importCSV.parseCSV('string;integer;real\nstring;1;1.0\nstring2;0;-1.4\n',';');
      var defJson = {string:'',integer:'',real:''};
      var ja = importCSV.convertArrToJSON(array, defJson);
      should(ja).match([{string:'string',integer:'1',real:'1.0'},{string:'string2',integer:'0',real:'-1.4'}]);
    });
    it('should handle integer types', function() {
      var array = importCSV.parseCSV('a;b;c\n1;10000;-15',';');
      var defJson = {a:0,b:0,c:0}
      var ja = importCSV.convertArrToJSON(array, defJson);
      should(ja).match([{a:1,b:10000,c:-15}]);
    });
    it('should handle float types', function() {
      var array = importCSV.parseCSV('a;b;c\n1.2;10000.1;-15.2',';');
      var defJson = {a:0,b:0,c:0}
      var ja = importCSV.convertArrToJSON(array, defJson);
      should.deepEqual({a:1.2,b:10000.1,c:-15.2},ja[0]);
    });
    it('should handle date types', function() {
      var array = importCSV.parseCSV('a;b;c\n2014-10-12;2000-01-01;0000-00-00',';');
      var defJson = {a:new Date(),b:new Date(),c:new Date()}
      var ja = importCSV.convertArrToJSON(array, defJson);
      should.deepEqual(new Date(2014,10-1,12,0,0,0),ja[0].a);
      should.deepEqual(new Date(2000,1-1,1,0,0,0),ja[0].b);
      should.deepEqual(new Date(0,0-1,0,0,0,0),ja[0].c);
    });
    it('should handle defaults', function() {
     var array = importCSV.parseCSV('string;integer;real\nstring;1;1.0\nstring2;0;-1.4\n',';');
      var defJson = {defaultValue:"default"}
      var ja = importCSV.convertArrToJSON(array, defJson);
      should.deepEqual("default",ja[0].defaultValue);
    });
  });
  context('readCSV',function() {
    var filename = "testfile.csv";
    afterEach(function(bddone) {
      fs.exists(filename, function(err) {
        if (err) {
          fs.unlink(filename);
        }
        bddone();
      })
    })
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
        db = configuration.getMongoDB();
        var a = importCSV.readCSVMongoDB('NonExistingFile.csv',db,{},function(err,data) {
           should.equal(err.errno,34);
           done();
        })
      });
      it('should load 2 datasets' , function(done) {
        fs.writeFileSync(filename,"name;count\na;2\nb;10");
        var a = importCSV.readCSVMongoDB(filename,db,{name:"",count:0},function(err,data) {
          should.equal(err,null);
          should.equal(data,"Datensätze: 2");
          db.collection("DataCollection").find({}).toArray(function(err,data) {
            should.equal(data.length,2);
            should.equal(data[0].name,'a');
            should.equal(data[0].count,'2');
            should.equal(data[1].name,'b');
            should.equal(data[1].count,'10');
          })
          done();
        })
      });
      it('should handle empty Files' ,function (done) {
        fs.writeFileSync(filename,"");
        var a = importCSV.readCSVMongoDB(filename,db,{name:"",count:0},function(err,data) {
          should.equal(err,"empty file");
          done();
        });
      })
      it('should generate an error on column numbers differs',function(bddone){
        fs.writeFileSync(filename,"name;count\na;2\nb;10\c;4;5");
        var a = importCSV.readCSVMongoDB(filename,db,{name:"",count:0},function(err,data) {
          should.exist(err);
          should(err).match(/^Invalid CSV File, Number of Columns differs/);
          bddone();
        })
      })

    });
    describe('readCSVpostgres',function() {
      before(function(bddone) {
        configuration.initialisePostgresDB();
        pg.connect(configuration.postgresConnectStr, function(err,client,pgdone){
          should.equal(err,null);
          async.series([
            DataCollection.dropTable,
            DataCollection.createTable
          ],function(err) {
            should.equal(null,err);
            pgdone();
            bddone();
          });
        });
      });
      it('should fail with no filename' , function(done) {
        var a = importCSV.readCSVPostgresDB('NonExistingFile.csv',{},function(err,data) {
          should.equal(err.errno,34);
          done();
        })
      });
      it('should load 2 datasets' , function(done) {
        fs.writeFileSync(filename,"schluessel;count\na;2\nb;10");
        var a = importCSV.readCSVPostgresDB(filename,{name:"",count:0},function(err,data) {
          should.equal(data,"Datensätze: 2");
          should.equal(err,null);
          pg.connect(configuration.postgresConnectStr, function(err,client,pgdone){
            should.equal(err,null);
            client.query("select key,count from DataCollection;",function(err,data){
              should.equal(err,null);
              should.notEqual(null,data);
              should.equal(data.rows.length,2);
              should.equal(data.rows[0].key,'a');
              should.equal(data.rows[0].count,'2');
              should.equal(data.rows[1].key,'b');
              should.equal(data.rows[1].count,'10');
              done();
              pgdone();
              client.end();
              });
            });
        })
      });
      it('should handle empty Files' ,function (done) {
        fs.writeFileSync(filename,"");
        var a = importCSV.readCSVPostgresDB(filename,{name:"",count:0},function(err,result) {
          should.equal(err,"empty file");
          done();
        })
      })
      it('should generate an error on column numbers differs',function(bddone){
        fs.writeFileSync(filename,"name;count\na;2\nb;10\c;4;5");
        var a = importCSV.readCSVPostgresDB(filename,{name:"",count:0},function(err,data) {
          should.exist(err);
          should(err).match(/^Invalid CSV File, Number of Columns differs/);
          bddone();
        })
      })

    });

  })
});
