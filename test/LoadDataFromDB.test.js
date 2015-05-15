var should = require('should');
var fs     = require("fs");
var path   = require('path');
var async  = require('async');


var config = require('../configuration.js');
var lod           = require('../model/LoadDataFromDB.js');
var OSMData       = require('../model/OSMData.js');
var helper = require('./helper.js');

describe('LoadDataFromDB', function () {
  beforeEach(function(bddone){
    helper.initUnallowedGlobals();
    bddone();
  })
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  before(function(bddone){
    async.series([
       OSMData.dropTable.bind(OSMData),
       OSMData.createTable.bind(OSMData)
      ],function(err) {
        should.not.exist(err);
        bddone();
      });
  })
  describe('insertValues',function()
  {
    var CC_Type  = {};
    CC_Type.map  = {"123":1};
    CC_Type.list = [];
		beforeEach(function() {
      CC_Type.matchKey= {boundary:["postal_code","administrative"]}
      CC_Type.map  = {"123":1};
      CC_Type.list = [];
      var undef;
      CC_Type.blaetterIgnore = undef;
		});
    it ('should ignore undefined Key', function() {
      var osmdoc = {boundary:"postal_code"}
      var key;
      should.not.exist (key);
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1});
      should.equal(CC_Type.list.length,0);
    });
    it ('should ignore undefined osmdoc.name', function() {
      var osmdoc = {boundary:"postal_code"}
      var key="23";
      should.not.exist (osmdoc.name);
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1});
      should.equal(CC_Type.list.length,0);
    });
    it ('should ignore wrong osmtype (List of Values)', function() {
      var osmdoc = {boundary:"something special",name:"halöle"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1});
      should.equal(CC_Type.list.length,0);
    });
    it ('should ignore wrong osmtype (values)', function() {
      CC_Type.matchKey = {bounday:"administrative"};
      var osmdoc = {boundary:"something special",name:"halöle"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1});
      should.equal(CC_Type.list.length,0);
    });
    it('should insert osmtype (List of Values)', function() {
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      var osmdoc = {boundary:"administrative",name:"New Border"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,"23":{name:"New Border",typ:"-"}});
      should(CC_Type.list).match(["23"]);
    });
    it ('should insert osmtype (values)', function() {
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,"23":{name:"New Border",typ:"country"}});
      should(CC_Type.list).match(["23"]);
    });
    it ('should insert osmtype with ending 0 multiple', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="230000";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,
                                    "230000":{name:"New Border",typ:"country"},
                                    "23000":{name:"New Border",typ:"country"},
                                    "2300":{name:"New Border",typ:"country"},
                                    "230":{name:"New Border",typ:"country"},
                                    "23":{name:"New Border",typ:"country"}});
      should(CC_Type.list).match(["230000"]);
    });
    it ('should should use the blaetter ignore list (ignore case)', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,
                                    "0":{name:"New Border",typ:"country"}});
      should(CC_Type.list).match([]);
    });
    it ('should should use the blaetter ignore list (pass case)', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,
                                    "0":{name:"New Border",typ:"state"}});
      should(CC_Type.list).match(["0"]);
    });
    it ('should handle multiple keys (positive)', function() {
      CC_Type.matchKey = {boundary:"administrative",osmcount_country:"DE"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3",osmcount_country:"DE"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1,
                                    "0":{name:"New Border",typ:"state"}});
      should(CC_Type.list).match(["0"]);
    });
    it ('should handle multiple keys (negative)', function() {
      CC_Type.matchKey = {boundary:"administrative",osmcount_country:"DE"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      var osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3",osmcount_country:"AT"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      should(CC_Type.map).match({"123":1});
      should(CC_Type.list).match([]);
    });
  });
  describe('sortAndReduce',function()
  {
  	list = ['1','12','120','120','13','121','134','135'];
  	lod.sortAndReduce(list);
  	should(list).match(['120','121','134','135']);
  });
  describe('initialise',function() {
    var db;
    var data;
    before(function(done) {
      var filename = path.resolve(__dirname, "LoadDataFromDB.test.json");
      var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      try {
        data = JSON.parse(filestring);
      } catch(err) {
        if (err) {
          console.log("Error Parsing: "+filename);
          throw(err);
        }
      }
      should.exist(data);
      var filename = path.resolve(__dirname, "OSMBoundaries.test.json");
      OSMData.initialise('postgres');
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      OSMData.import(filename,function(err,data){
        should.not.exist(err);
        should(data).equal("Datensätze: 6");
        lod.initialise(done);
      })
    });
    it('should read the Data DE_RGS',function() {
      should(lod.DE_RGS.map).match(data.DE_RGS.map);
      should(lod.DE_RGS.list).match(data.DE_RGS.list);
    });
    it('should read the Data DE_PLZ',function() {
      should(lod.DE_PLZ.map).match(data.DE_PLZ.map);
      should(lod.DE_PLZ.list).match(data.DE_PLZ.list);
    });
    it('should read the Data DE_AGS',function() {
      should(lod.DE_AGS.map).match(data.DE_AGS.map);
      should(lod.DE_AGS.list).match(data.DE_AGS.list);
    });
    it('should read the Data AT_AGS',function() {
      should(lod.AT_AGS.map).match(data.AT_AGS.map);
      should(lod.AT_AGS.list).match(data.AT_AGS.list);
    });
  });
});
