var debug    = require('debug')('POIReader');
var fs       = require("fs");
var path     = require('path');
var request  = require('request');
var es          = require('event-stream');
var should  = require('should');

var wochenaufgabe    = require('./wochenaufgabe.js');
var config           = require('./configuration.js');
var loadDataFromDB   = require('./model/LoadDataFromDB.js');
var loadOverpassData = require('./model/LoadOverpassData.js');

var POI = require('./model/POI.js');
var DataCollection = require('./model/DataCollection.js');


var async    = require('async');


var assert = require('assert');

function cleanObject(obj) {
  debug('cleanObject');
  for (var k in obj) {
    if (typeof (obj[k]) =='object') {
      cleanObject(obj[k]);
    }
    if (k.indexOf(".") >0 ) {
     	delete obj[k];
     }
  }
}

function cleanTags(elementList) {
  debug("cleanTags"); 
  for (var i = 0;i<elementList.length;i++) {
    var data = elementList[i].tags;
    cleanObject(data);
  } 
}

var tagsToCopy = 
[
  "de:regionalschluessel",
  "de:amtlicher_gemeindeschluessel",
  "ref:at:gkz",
  "postal_code",
  "swisstopo:KANTONSNUM",
  "swisstopo:BEZIRKSNUM",
  "swisstopo:BFS_NUMMER",
  "admin_level"];

exports.prepareData = function prepareData(data) {
  debug("prepareData");
  // First collect all Area Numbers in result
  var areaList = {};
  var actualElement;
  var result = [];
  var timestamp_osm_base = data.osm3s.timestamp_osm_base;

  debug("Data to be parsed %s",data.elements.length);
  for (var i = 0;i<data.elements.length;i++) {
    var element = data.elements[i];
 
    switch (element.type) {
      case "node":
      case "way":
      case "relation":
        {
          actualElement = element;
          actualElement.timestamp_osm_base = timestamp_osm_base;
          result.push(actualElement);
          break;
        }
      case "area":
      {
        var areaID = element.id;
        if (typeof (element.tags) == 'undefined' ) {
          if (typeof (areaList[areaID]) == 'undefined') {
            areaList[areaID] = {};
            //areaList[areaID].id = areaID;
            //areaList[areaID].type = "area";
          }
          if (typeof(actualElement.osmArea) == 'undefined') {
            actualElement.osmArea = [];
          }
          actualElement.osmArea.push(areaList[areaID]); 
        } else {
          if (typeof (areaList[areaID]) == 'undefined') {
            areaList[areaID] = {};
            //areaList[areaID].id = areaID;
            //areaList[areaID].type = "area";
          }
          for (var j = 0;j<tagsToCopy.length;j++) {
            var k = tagsToCopy[j];

            if (typeof(element.tags[k]) != 'undefined') {
               areaList[areaID][k] = element.tags[k];
            }
          }
        }
      }
    }
  }
  data.elements= result;
  return data;
}
function getPOIOverpass(wa,cb) {
	debug("getPOIOverpass %s",wa.name);
	var filename = wa.name+".json";
  var testMode = false;
  if (typeof (process.env.NODE_ENV) != 'undefined' && process.env.NODE_ENV == 'test') {
    testMode = true;
  }



 /* caching der OSM Ergebnisse erstmal ausgeblendet
  // Check, wether a file with the name exist
  // than use that data
	filename = path.resolve(__dirname, filename);
	if (!testMode && fs.existsSync(filename)) {
		debug("Loading Data from "+filename);
		var result = fs.readFileSync(filename);
		var data = JSON.parse(result);
		debug("Loaded Elemnts %s:"+data.elements.length,country);
		cleanTags(data.elements);

		cb(null, exports.prepareData(data));
		return;
	} */

  // Start Overpass Query
	console.log("Overpass Abfrage Starten für "+wa.osmArea + " "+ (new Date()));

	loadOverpassData.overpassQuery(wa.overpass.fullQuery, function(err,result) {
		debug("getPOIOverpass->CB %s",wa.name);
		console.log("Overpass Abfrage Beendet für "+wa.osmArea + " " + (new Date()));
		if (err) {
			console.log("Fehler: "+JSON.stringify(err));
			console.log(JSON.stringify(result));
			cb(err,null);
		} else {
			//Skip Writing to file
      //fs.writeFileSync(filename,result);
			var data = JSON.parse(result);
			debug("Loaded Elements:"+data.elements.length);
     	cleanTags(data.elements);
			cb(null,exports.prepareData(data));
		}
	})
}



function splitOverpassResult(wa,data,cb) {
	debug("splitOverpassResult %s",wa.name);

 
	var list = {};
	debug("Elemente geladen: "+data.length+" für "+wa.name);


	for (var i =0;i<data.elements.length;i++) {
		var element = data.elements[i];
		var keyIntern = element.type + element.id;
		element.overpass = {};
		element.overpass["loadBy"] = wa.country_code;
		list[keyIntern] = element;
	}
  // if it is no pharmacy WA the code has to be changed.

  should(wa.description).equal("Apotheke");
  var query = "where data->'overpass'->>'loadBy' ='"+wa.country_code+"' and data->'tags'->>'amenity' ='pharmacy' ";
	POI.find(query,function(err,result) {
	  debug("getPOIByPLZMongo->CB %s",wa.name);
		if (err) {
			console.log("Fehler "+err);
			cb(err,null);
      return;
		} else {
			var remove = [];
			var update = [];
			var insert = [];
      var unchanged = 0;
			debug("Found "+result.length+ " POIs in DB");
			for (var i=0;i<result.length;i++ ) {
				element = result[i];
				var key = element.type + element.id;
				if (typeof(list[key])=='undefined') {
					// Element not found in Overpass Result
					// Remove it.
					remove.push(element);
				} else {
					// Element found in MongoDB and Overpass
					// Please Update it if necessary
					list[key]._id = element._id;
					if (list[key].version != element.version) {
						update.push(list[key]);
					} else {
            unchanged += 1;
          }
				}
			}
			// Check all not handled overpass result for insert
			for (var k in list) {
				if (typeof(list[k]._id) == 'undefined') {
					insert.push(list[k]);
				}
			}
			var erg = {};
			debug("To be removed: "+remove.length);
			debug("To be updated: "+update.length);
			debug("To be inserted: "+insert.length);
      debug("Unchanged: " +unchanged);
			erg.remove = remove;
			erg.update = update;
			erg.insert = insert;
 			cb(null,erg);
		}
	}) 
}

function removePOIFromPostgres(wa,remove,cb) {
  debug("removePOIFromPostgres %s",wa.name);
  if (remove.length==0) {
  	cb(null,null);
    debug('remove: nothing to do');
  	return;
  }
  debug("To Be Removed: "+remove.length + " DataSets");

  var q = async.queue(function (task, cb) {
    debug("removePOIFromPostgres->Queue");
    POI.remove({type:task.data.type,id:task.data.id}, function(err,result) {
      debug("removePOIFromMongo->MongoCB");
      if (err) {
    	console.log("Error "+err);
    	cb(err,null);
      } else {
    	cb (null,null);
      }
  	})
  }, 2);
  q.pause();

  q.drain = function() {
    cb(null,null)
  }

// add some items to the queue
  for (i=0;i<remove.length;i++) {
  	q.push({data:remove[i]});
  }
  q.resume();
}


function updatePOIFromPostgres(wa,update,cb) {
  debug("updatePOIFromPostgres %s",wa.name);
  if (update.length==0) {
    debug("update: nothing to do");
  	cb(null,null);
  	return;
  }
   debug("updatePOIFromPostgres.1");
  cleanTags(update);
  debug("To Be Updated: "+update.length + " DataSets");

  var q = async.queue(function (task, cb) {
    debug("updatePOIFromMongo->Queue");

    POI.save(task.data, function(err,result) {
      debug("updatePOIFromMongo->MongoCB");
    if (err) {
    	console.log("Error: "+err);
    	cb(err,null);
      } else {
    	cb (null,null);
      }
  	})
  }, 10);
  q.pause();

  q.drain = function() {
    cb(null,null)
  }

// add some items to the queue
  for (i=0;i<update.length;i++) {
  	q.push({data:update[i]});
  }
  q.resume();
}

function insertPOIFromPostgres(wa,insert,cb) {
  debug("insertPOIFromPostgres %s",wa.name);
  debug("To Be Inserted: "+insert.length + " DataSets");
  if (insert.length == 0) {
    debug('Insert: Nothing to do');
  	cb(null,null);
  	return;
  }
  POI.insertData(insert, function(err,result) {
    debug("insertPOIFromPostgres->CB");
    if (err) {
    	console.log("Error "+ err);
    	cb(err,null);
    } else {
    	cb (null,null);
    }
  })
}




var nominatimMapQuestUrl = "http://open.mapquestapi.com/nominatim/v1/reverse.php";
var nominatimNominatimUrl = "http://nominatim.openstreetmap.org/reverse";

var nominatimUrl = nominatimMapQuestUrl;

function nominatim(callback,result) {
  debug("nominatim");
  var q = async.queue(function(task,cb) {
    POI.find("where ((data->'nominatim'->'timestamp') is  null)  limit 1", function(err, objList) {
      debug("nominatim->CB");
      if (err) {
        console.log("Error occured in function: QueueWorker.getNextJob");
        console.log(err);
        cb(err,null);
        return;
      }
      var obj;
      if (objList.length == 1) {
        obj = objList[0];

        debug("found job %s %s",obj.type, obj.id);
        osm_type="";
    		switch (obj.type) {
    		  case "node": osm_type = "osm_type=N";
    					 break;
    		  case "way": osm_type = "osm_type=W";
    					 break;
    		  case "relation": osm_type = "osm_type=R";
    					 break;
    		}
    		// return obj as job object
    		url = nominatimUrl+"?format=json&";
    		url += osm_type;
    		url += "&osm_id="+obj.id+"&zoom=18&addressdetails=1";
    		url += "&email=OSMUser_TheFive";
    		request.post(url , function (error, response, body) {
    		  if (error) {
      			console.log("Error "+error);
      			task.q.push({q:q});
      			cb (err,null);
      			return;
    		  }
    		  if (response.statusCode==200) {
    			elementData = JSON.parse(body);
    			var date = new Date();

    			if (typeof(elementData.error)=='undefined') {
    				elementData.address.timestamp = date;
    				obj.nominatim = elementData.address;
    			} else {
    				elementData.timestamp = date;
    				obj.nominatim = elementData;
    			}
    			cleanObject(obj);
    			POI.save(obj, function(err,result) {
    			  debug("nominatim->save");
    			  if (err) {
      				console.log("Error "+err);
      				console.dir(obj);
      				task.q.push({q:q});
      				cb(err,null);
      				return;
    			  } else {
      				task.q.push({q:q});
      				cb (null,null);
      				return;
    			  }
    			})
    		  }
    		})
      } else callback(null,null)
	})
  },1);
  q.push({q:q});
}

function countPOI(wa,data,cb) {
  debug('countPOI %s',wa.name);
  var defJson = {};
  defJson.measure = wa.name;
 
  if (wa == null) {
    cb("Keine Wochenaufgabe in count POI");
    return;
  }
  defJson.schluessel = "undefined";
  defJson.timestamp = data.osm3s.timestamp_osm_base;
  
  var result = wa.tagCounterGlobal(data.elements,wa.map.list,wa.key,defJson);
  async.eachSeries(result,DataCollection.save.bind(DataCollection),cb);
  return;
}
 

exports.doReadPOI = function doReadPOI(measure,callback) {
  debug('doReadPOI %s',measure);

  var wa = wochenaufgabe.map[measure];

  async.auto( {
    config: function(cb) {config.initialise(cb)},
    overpass: ["config",function(cb){getPOIOverpass(wa,cb)}],
    sor:["overpass",function(cb,r){splitOverpassResult(wa,r.overpass,cb)}],
    update: ["sor",function(cb,r) {updatePOIFromPostgres(wa,r.sor.update,cb)}],
    insert: ["sor",function(cb,r) {insertPOIFromPostgres(wa,r.sor.insert,function() {cb()})}],
    remove: ["sor",function(cb,r) {removePOIFromPostgres(wa,r.sor.remove,cb)}],
    count: ["overpass",function(cb,r) {countPOI(wa,r.overpass,cb)}]
  },
  function(err,results) {
    debug('doReadPOI->CB');
    if (err) {
      console.log("doReadPOI Error for "+wa.osmArea+" measure "+ measure+" "+JSON.stringify(err));
      callback(err);
      return;
    }
    nominatim(callback);
  });
}