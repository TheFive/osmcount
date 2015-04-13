var pg     = require('pg');
var fs     = require('fs');
var path   = require('path');
var async  = require('async');
var should = require('should');

var config     = require('../configuration.js');
var DataTarget = require('../model/DataTarget.js')


describe('DataTarget', function() {
  beforeEach(function(bddone) {
    DataTarget.initialise('postgres');
    async.series([
      DataTarget.dropTable.bind(DataTarget),
      DataTarget.createTable.bind(DataTarget)
    ],function(err) {
      if (err) console.dir(err);
      should.equal(null,err);
      bddone();
    });
  });
  describe('import',function(bddone) {
    it('should import data',function(bddone){
      var filename = path.resolve(__dirname, "DataTarget.test.json");
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      DataTarget.import(filename,function(err,data){
        if (err) console.dir(err);
        should.not.exist(err,null);
        should.equal(data,"Datensätze: 2");
        bddone();
      });
    });
    it('should handle connection String Problems.',function(bddone){
      var cstr = config.postgresConnectStr;
      config.postgresConnectStr = "murks";
      var filename = path.resolve(__dirname, "DataTarget.test.json");   
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});      
      DataTarget.import(filename,function(err,data){
        should.exist(err);
        // Do not forget to restore connection String
        config.postgresConnectStr = cstr;
        bddone();
      });
    });

  });
  describe('importCSV',function() {
    it('should throw an err, if function is called',function(bddone){
      DataTarget.importCSV("Filename.csv",{name:"string"},function(err,data){
        should(err).equal('No importCSV implemented');
        bddone();
      });
    });
  })
  describe('aggregate',function(bddone) {
    it('aggretate two values',function(bddone){
      var filename = path.resolve(__dirname, "DataTarget.test.json");
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      DataTarget.import(filename,function(err,data){
        should.not.exist(err);
        should.equal(data,"Datensätze: 2");
        var param = {measure:"Apotheke",lengthOfKey:1};
        DataTarget.aggregate(param,function(err,data){
          should.not.exist(err);
          should(data.length).equal(1);
          should(data[0].vorgabe).equal(0.257);
          bddone();
        })
      });
    });
  });
})
