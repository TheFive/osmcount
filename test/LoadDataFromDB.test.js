var assert =require('assert');
var lod = require('../LoadDataFromDB.js');
var configuration = require('../configuration.js');
var dbHelper = require('../test/dbHelper.js');
var fs   = require("fs");
var path = require('path');

describe('LoadDataFromDB', function () {
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
      osmdoc = {boundary:"postal_code"}
      var key;
      assert.equal (typeof(key) ,'undefined');
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.equal(CC_Type.list.length,0);
    });
    it ('should ignore undefined osmdoc.name', function() {
      osmdoc = {boundary:"postal_code"}
      var key="23";
      assert.equal (typeof(osmdoc.name) ,'undefined');
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.equal(CC_Type.list.length,0);
    });
    it ('should ignore wrong osmtype (List of Values)', function() {
      osmdoc = {boundary:"something special",name:"halöle"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.equal(CC_Type.list.length,0);
    });
    it ('should ignore wrong osmtype (values)', function() {
      CC_Type.matchKey = {bounday:"administrative"};
      osmdoc = {boundary:"something special",name:"halöle"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.equal(CC_Type.list.length,0);
    });
    it('should insert osmtype (List of Values)', function() {
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      osmdoc = {boundary:"administrative",name:"New Border"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,"23":{name:"New Border",typ:"-"}});
      assert.deepEqual(CC_Type.list,["23"]);
    });
    it ('should insert osmtype (values)', function() {
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,"23":{name:"New Border",typ:"country"}});
      assert.deepEqual(CC_Type.list,["23"]);
    });
    it ('should insert osmtype with ending 0 multiple', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="230000";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,
                                    "230000":{name:"New Border",typ:"country"},
                                    "23000":{name:"New Border",typ:"country"},
                                    "2300":{name:"New Border",typ:"country"},
                                    "230":{name:"New Border",typ:"country"},
                                    "23":{name:"New Border",typ:"country"}});
      assert.deepEqual(CC_Type.list,["230000"]);
    });
    it ('should should use the blaetter ignore list (ignore case)', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"2"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,
                                    "0":{name:"New Border",typ:"country"}});
      assert.deepEqual(CC_Type.list,[]);
    });
    it ('should should use the blaetter ignore list (pass case)', function() {
      CC_Type.matchKey = {boundary:"administrative"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,
                                    "0":{name:"New Border",typ:"state"}});
      assert.deepEqual(CC_Type.list,["0"]);
    });
    it ('should handle multiple keys (positive)', function() {
      CC_Type.matchKey = {boundary:"administrative",osmcount_country:"DE"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3",osmcount_country:"DE"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1,
                                    "0":{name:"New Border",typ:"state"}});
      assert.deepEqual(CC_Type.list,["0"]);
    });
    it ('should handle multiple keys (negative)', function() {
      CC_Type.matchKey = {boundary:"administrative",osmcount_country:"DE"};
      CC_Type.secondInfoKey = "admin_level";
      CC_Type.secondInfoValueMap = {"1":"world","2":"country","3":"state"};
      CC_Type.blaetterIgnore = [{admin_level:2}]
      osmdoc = {boundary:"administrative",name:"New Border",admin_level:"3",osmcount_country:"AT"}
      var key="0";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.deepEqual(CC_Type.list,[]);
    });
  });
  describe('sortAndReduce',function() 
  {
  	list = ['1','12','120','120','13','121','134','135'];
  	lod.sortAndReduce(list);
  	assert.deepEqual(list,['120','121','134','135']);
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
      assert.notEqual(typeof(data),'undefined');
      assert.notEqual(0,data.length);
      configuration.initialiseDB( function() {
        db = configuration.getDB();
        dbHelper.prepareCollection(db,"OSMBoundaries","OSMBoundaries.test.json",function()
        { lod.initialise(done);});
      });
    });
    it('should read the Data DE_RGS',function() {
      assert.deepEqual(lod.DE_RGS.map, data.DE_RGS.map,"DE_RGS maps not the same");
      assert.deepEqual(lod.DE_RGS.list,data.DE_RGS.list,"DE_RGS list not the same");
    });
    it('should read the Data DE_PLZ',function() {
        assert.deepEqual(lod.DE_PLZ.map, data.DE_PLZ.map,"DE_PLZ maps not the same");
        assert.deepEqual(lod.DE_PLZ.list,data.DE_PLZ.list,"DE_PLZ list not the same");
    });
    it('should read the Data DE_AGS',function() {   
        assert.deepEqual(lod.DE_AGS.map, data.DE_AGS.map,"DE_AGS map not the same");
        assert.deepEqual(lod.DE_AGS.list,data.DE_AGS.list,"DE_AGS list not the same");
    });
    it('should read the Data AT_AGS',function() {            
        assert.deepEqual(lod.AT_AGS.map, data.AT_AGS.map,"AT_AGS map not the same");
        assert.deepEqual(lod.AT_AGS.list,data.AT_AGS.list,"AT_AGS list not the same");
    });
  });
});