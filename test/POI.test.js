var pg=require('pg');
var fs = require('fs');
var path=require('path');
var async = require('async');
var should = require('should');

var config = require('../configuration.js');
var POI = require('../model/POI.js');
<<<<<<< HEAD
var dbHelper = require('./dbHelper.js');


describe('POI', function() {
  describe('import',function(bddone) {
    describe('should find data',function(){
      before(function(bddone){
        dbHelper.initialiseTablePostgres(POI,"POI.test.json",bddone);
=======
var helper = require('./helper.js');


describe('POI', function() {
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  describe('import',function(bddone) {
    describe('should find data',function(){
      before(function(bddone){
        helper.initialiseTablePostgres(POI,"POI.test.json",bddone);
>>>>>>> develop
      });
      it('should import POI.test.json',function(bddone){
        POI.find({},function(err,data){
          should.not.exist(err);
          should(data.length).equal(7);
          bddone();
        })        
      })
      it('should find pharmacies in AT',function(bddone){
        POI.find({tags:{amenity:"pharmacy"},overpass:{country:"DE"}},function(err,data){
          should.not.exist(err);
          should(data.length).equal(1);
          should(data[0].tags).eql({
              "addr:city": "Graz",
              "addr:country": "AT",
              "addr:housenumber": "19",
              "addr:postcode": "8020",
              "addr:street": "Wiener Straße",
              "amenity": "pharmacy",
              "dispensing": "yes",
              "name": "Löwen-Apotheke",
              "phone": "+43 316 714691",
              "wheelchair": "no"
            })
          bddone();

        })
      })
    })
  });
  describe('remove',function() {
    before(function(bddone) {
<<<<<<< HEAD
      dbHelper.initialiseTablePostgres(POI,"POI.test.json",bddone);
=======
      helper.initialiseTablePostgres(POI,"POI.test.json",bddone);
>>>>>>> develop
    })
    it('should remove a node',function (bddone) {
      POI.remove({type: "node","id": 39663366},function(err,data) {
        should.not.exist(err);
        POI.find({overpass:{country:"DE"}},function(err,data){
          should.not.exist(err);
          should(data.length).equal(0);
          bddone();
        })
      })
    })

  })
<<<<<<< HEAD
  describe.only('insertData',function() {
    before(function(bddone) {
      dbHelper.initialiseTablePostgres(POI,bddone);
=======
  describe('insertData',function() {
    before(function(bddone) {
      helper.initialiseTablePostgres(POI,bddone);
>>>>>>> develop
    })
    it('should insert 4 JSON Objects',function (bddone) {
      POI.insertData([{type: "node","id": 39663366},
                     {type:"node","id": 1,a:1},
                     {type:"node","id": 2,a:1},
                     {type:"way","id": 3,a:3}],
                     function(err,data) {
        should.not.exist(err);
        POI.find({},function(err,data){
          should.not.exist(err);
          should(data.length).equal(4);
          should(data).containDeep([{type: "node","id": 39663366},
                     {type:"node","id": 1,a:1},
                     {type:"node","id": 2,a:1},
                     {type:"way","id": 3,a:3}]);
          bddone();
        })
      })
    })

  })
  describe('save',function() {
    before(function(bddone) {
<<<<<<< HEAD
      dbHelper.initialiseTablePostgres(POI,"POI.test.json",bddone);
=======
      helper.initialiseTablePostgres(POI,"POI.test.json",bddone);
>>>>>>> develop
    })
    it('should save a node',function (bddone) {
      POI.find({overpass:{country:"DE"}},function(err,data) {
        should.not.exist(err);
        should(data.length).equal(1);
        var poi = data[0];
        poi.overpass.country = "TEST";
        POI.save(poi,function(err,cb){
          should.not.exist(err);
          POI.find({overpass:{country:"DE"}},function(err,data) {
            should.not.exist(err);
            should(data.length).equal(0);
            POI.find({overpass:{country:"TEST"}},function(err,data) {
              should.not.exist(err);
              should(data.length).equal(1);
              bddone();
            })
          })
        })
      })
    })
  })
})
