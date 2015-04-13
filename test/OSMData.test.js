var pg=require('pg');
var fs = require('fs');
var path=require('path');
var async = require('async');
var should = require('should');

var config = require('../configuration.js');
var OSMData = require('../model/OSMData.js')


describe('OSMData', function() {
  describe('import',function(bddone) {

    beforeEach(function(bddone) {
      async.series([
        config.initialisePostgresDB,
        OSMData.dropTable.bind(OSMData),
        OSMData.createTable.bind(OSMData)
      ],function(err) {
        if (err) console.dir(err);
        should.equal(null,err);
        bddone();
      });
    });
    it('should import data',function(bddone){
      var filename = path.resolve(__dirname, "OSMBoundaries.test.json");
      OSMData.initialise('postgres');
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      OSMData.import(filename,function(err,data){
        if (err) console.dir(err);
        should.not.exist(err,null);
        should.equal(data,"Datensätze: 6");
        bddone();
      });
    });
  });
})
