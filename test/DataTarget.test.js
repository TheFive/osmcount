var pg=require('pg');
var fs = require('fs');
var path=require('path');
var async = require('async');
var should = require('should');

var configuration = require('../configuration.js');
var DataTarget = require('../model/DataTarget.js')


describe('DataTarget', function() {
  describe('import',function(bddone) {

    beforeEach(function(bddone) {
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
  });
})
