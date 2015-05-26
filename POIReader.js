var debug    = require('debug')('POIReader');
var fs       = require("fs");
var path     = require('path');
var request  = require('request');
var es          = require('event-stream');

var config           = require('./configuration.js');
var loadDataFromDB   = require('./model/LoadDataFromDB.js');
var loadOverpassData = require('./model/LoadOverpassData.js');

var POI = require('./model/POI.js');


var async    = require('async');




var query =
{  DE: '[out:json][timeout:3600];area[name="Deutschland"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["de:amtlicher_gemeindeschluessel"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["de:amtlicher_gemeindeschluessel"]; \
          out;',
 AT: '[out:json][timeout:3600];area[name="Österreich"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:at:gkz"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["ref:at:gkz"]; \
          out;',
CH: '[out:json][timeout:3600];area[name="Schweiz"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:bfs_Gemeindenummer"];out ids; ); \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["ref:bfs_Gemeindenummer"]; \
          out;',
}

function cleanObject(obj) {
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
  "ref:at:gkz"]

exports.prepareData = function prepareData(data) {
  debug("prepareData");
  // First collect all Area Numbers in result
  var areaList = {};
  var actualElement;
  var result = [];
  debug("Data to be parsed %s",data.length);
  for (var i = 0;i<data.length;i++) {
    var element = data[i];
 
    switch (element.type) {
      case "node":
      case "way":
      case "relation":
        {
          actualElement = element;
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
  return result;
}
function getPOIOverpass(country,cb) {
	debug("getPOIOverpass");
	var filename = country+".json";

  // Check, wether a file with the name exist
  // than use that data
	filename = path.resolve(__dirname, filename);
	if (fs.existsSync(filename)) {
		debug("Loading Data from "+filename);
		result = fs.readFileSync(filename);
		var data = JSON.parse(result);
		debug("Loaded Elemnts:"+data.elements.length);
		cleanTags(data.elements);

		cb(null, exports.prepareData(data));
		return;
	}

  // Start Overpass Query
	console.log("Overpass Abfrage Starten für "+country+ (new Date()));

	loadOverpassData.overpassQuery(query[country], function(err,result) {
		debug("getPOIOverpass->CB");
		console.log("Overpass Abfrage Beendet für "+country + (new Date()));
		if (err) {
			console.log("Fehler: "+JSON.stringify(err));
			console.log(JSON.stringify(result));
			cb(err,null);
		} else {
			fs.writeFileSync(filename,result);
			var data = JSON.parse(result);
			debug("Loaded Elements:"+data.elements.length);
     	cleanTags(data.elements);
			cb(null,exports.prepareData(data));
		}
	})
}



function splitOverpassResult(country,data,cb) {
	debug("splitOverpassResult %s",country);

 
	var list = {};
	debug("Elemente geladen: "+data.elements.length+" für "+country);


	for (i =0;i<data.elements.length;i++) {
		var element = data.elements[i];
		var keyIntern = element.type + element.id;
		element.overpass = {};
		element.overpass["loadBy"] = country;
		list[keyIntern] = element;
	}

  var query = "where data->'overpass'->>'loadBy' ='"+country+"' and data->'tags'->>'amenity' ='pharmacy' ";
	POI.find(query,function(err,result) {
	  debug("getPOIByPLZMongo->CB %s",country);
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
				key = element.type + element.id;
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

function removePOIFromPostgres(country,remove,cb) {
  debug("removePOIFromPostgres %s",country);
  if (remove.length==0) {
  	cb(null,null);
    debug('remove: nothing to do');
  	return;
  }
  debug("To Be Removed: "+remove.length + " DataSets");

  var q = async.queue(function (task, cb) {
    debug("removePOIFromMongo->Queue");
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


function updatePOIFromPostgres(country,update,cb) {
  debug("updatePOIFromPostgres %s",country);
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

function insertPOIFromPostgres(country,insert,cb) {
  debug("insertPOIFromPostgres %s",country);
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

function nominatim(cb,result) {
  debug("nominatim");
  q = async.queue(function(task,cb) {
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
			date = new Date();

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
      }
	})
  },1);
  q.push({q:q});
  if (cb) cb(null,null);
}


exports.doReadPOI = function doReadPOI(callback) {


  debug("storePOI");

 
 async.auto( {
       config: config.initialise,
  
       overpassAT: ["config",function(cb){getPOIOverpass("AT",cb)}],
       overpassCH: ["config","overpassAT",function(cb){getPOIOverpass("CH",cb)}],
       overpassDE: ["config","overpassAT","overpassCH",function(cb){getPOIOverpass("DE",cb)}],

       sorDE:["overpassDE",function(cb,r){splitOverpassResult("DE",r.overpassDE,cb)}],
       sorAT:["overpassAT",function(cb,r){splitOverpassResult("AT",r.overpassAT,cb)}],
       sorCH:["overpassCH",function(cb,r){splitOverpassResult("CH",r.overpassCH,cb)}],

       updateDE: ["sorDE",function(cb,r) {updatePOIFromPostgres("DE",r.sorDE.update,cb)}],
       updateAT: ["sorAT",function(cb,r) {updatePOIFromPostgres("AT",r.sorAT.update,cb)}],
       updateCH: ["sorCH",function(cb,r) {updatePOIFromPostgres("CH",r.sorCH.update,cb)}],

       insertDE: ["sorDE",function(cb,r) {insertPOIFromPostgres("DE",r.sorDE.insert,cb)}],
       insertAT: ["sorAT",function(cb,r) {insertPOIFromPostgres("AT",r.sorAT.insert,cb)}],
       insertCH: ["sorCH",function(cb,r) {insertPOIFromPostgres("CH",r.sorCH.insert,cb)}],

       removeDE: ["sorDE",function(cb,r) {removePOIFromPostgres("DE",r.sorDE.remove,cb)}],
       removeAT: ["sorAT",function(cb,r) {removePOIFromPostgres("AT",r.sorAT.remove,cb)}],
       removeCH: ["sorCH",function(cb,r) {removePOIFromPostgres("CH",r.sorCH.remove,cb)}]
     },
 
  
       function(err,results) {
       	console.log("Postres Updated");
        console.log("Start Nominatim");
        nominatim();
        callback();
       });


}