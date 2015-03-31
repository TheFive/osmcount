var pg     = require('pg');
var fs     = require('fs');
var path   = require('path');
var async  = require('async');
var should = require('should');

var config     = require('../configuration.js');
var DataTarget = require('../model/DataTarget.js')


describe('DataTarget', function() {
  describe('import',function(bddone) {

    beforeEach(function(bddone) {
      DataTarget.initialise('postgres');
      async.series([
        DataTarget.dropTable,
        DataTarget.createTable
      ],function(err) {
        if (err) console.dir(err);
        should.equal(null,err);
        bddone();
      });
    });
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
})
