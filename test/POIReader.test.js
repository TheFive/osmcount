var pg=require('pg');
var fs = require('fs');
var path=require('path');
var async = require('async');
var should = require('should');

var POIReader = require('../POIReader.js');
var helper = require('./helper.js');


describe('POIReader', function() {
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  describe('prepareData',function(){
    it('should parse Data of Haan',function(bddone) {
      var filename = path.resolve(__dirname, "POIPharmacyHaan.json");
      var data = JSON.parse(fs.readFileSync(filename));
      var result = POIReader.prepareData(data);
      console.log(JSON.stringify(result,null,2));
      should(result.elements.length).equal(7);
      bddone();

    })
  })
})
