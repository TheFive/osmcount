var assert =require('assert');
var lod = require('../LoadDataFromDB.js');

describe('LoadDataFromDB', function () {
  describe('insertValues',function() 
  {
    var CC_Type  = {};
    CC_Type.map  = {"123":1};
    CC_Type.list = [];
    CC_Type.keyType = "boundary";
    CC_Type.keyValue = ["postal_code","administrative"];
		beforeEach(function() {
      CC_Type.map  = {"123":1};
      CC_Type.list = []; 
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
      CC_Type.keyType = "boundary";
      CC_Type.keyValue = "administrative";
      osmdoc = {boundary:"something special",name:"halöle"}
      var key="23";
      lod.insertValue(CC_Type,key,osmdoc);
      assert.deepEqual(CC_Type.map,{"123":1});
      assert.equal(CC_Type.list.length,0);
    });
    it ('should insert osmtype (List of Values)', function() {
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
      CC_Type.keyType = "boundary";
      CC_Type.keyValue = "administrative";
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
  });
  describe('sortAndReduce',function() 
  {
  	list = ['1','12','120','120','13','121','134','135'];
  	lod.sortAndReduce(list);
  	assert.deepEqual(list,['120','121','134','135']);
  });
});