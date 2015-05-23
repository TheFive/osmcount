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
{ DE:'[timeout:900][out:json];area["int_name"="Deutschland"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;',
 AT: '[timeout:900][out:json];area["int_name"="Österreich"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;',
 CH: '[timeout:900][out:json];area["int_name"="Schweiz"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;'
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

function getPOIOverpass(cb,result) {
	debug("getPOIByPLZOverpass");
	var country = result.country;
	var filename = country+".json";
	filename = path.resolve(__dirname, filename);
	if (fs.existsSync(filename)) {
		debug("Loading Data from "+filename);
		result = fs.readFileSync(filename);
		var data = JSON.parse(result);
		debug("Loaded Elemnts:"+data.elements.length);
		cleanTags(data.elements);
		cb(null, data);
		return;
	}
	console.log("Overpass Abfrage Starten für "+country);

	loadOverpassData.overpassQuery(query[country], function(err,result) {
		debug("getPOIByPLZOverpass->CB");
		console.log("Overpass Abfrage Beendet für "+country);
		if (err) {
			console.log("Fehler: "+JSON.stringify(err));
			console.log(JSON.stringify(result));
			cb(err,null);
		} else {
			fs.writeFileSync(filename,result);
			var data = JSON.parse(result);
			debug("Loaded Elements:"+data.elements.length);
     		cleanTags(data.elements);
			cb(null,data);
		}
	})
}



function getPOIByPLZMongo(cb,result) {
	debug("getPOIByPLZMongo");

	var country = result.country;
	var data = result.overpass;
 
	var list = {};
	debug("Elemente geladen: "+data.elements.length+" für "+country);
/*

	for (i =0;i<data.elements.length;i++) {
		var element = data.elements[i];
		var keyIntern = element.type + element.id;
		element.overpass = {};
		element.overpass["loadBy"] = country;
		list[keyIntern] = element;
	}
	var query = { "overpass.loadBy":country, "tags.amenity": "pharmacy"}

	POI.find(query,{},function(err,result) {
	  debug("getPOIByPLZMongo->CB");
		if (err) {
			console.log("Fehler "+err);
			cb(err,null);
		} else {
			var remove = [];
			var update = [];
			var insert = [];
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
			erg.remove = remove;
			erg.update = update;
			erg.insert = insert;
 			cb(null,erg);
		}
	}) */
var erg = {};
      erg.remove = [];
      erg.update = [];
      erg.insert = data.elements;
      cb(null,erg);
}

function removePOIFromPostgres(cb,result) {
  debug("removePOIFromPostgres");
  var remove = result.mongo.remove;
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


function updatePOIFromPostgres(cb,result) {
  debug("updatePOIFromPostgres");
  var update = result.mongo.update;
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

function insertPOIFromPostgres(cb,result) {
  debug("insertPOIFromPostgres");
  var insert = result.mongo.insert;
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
    POI.find("where ((data->'nominatim'->'timestamp') is not null) and (data->'nominatim'->>'timestamp')::timestamp >= now() limit 1", function(err, objList) {
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
        console.dir(obj);
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
			  debug("nominatim->MongoCB");
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
  cb(null,null);
}


  debug("storePOI");

  async.series([config.initialise, 
       function(cb) {
       async.auto( {
             country: function(cb,result) {cb(null,"CH")},
             overpass: ["country",getPOIOverpass],
             mongo:["overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromPostgres],
             insert: ["mongo",insertPOIFromPostgres],
             remove: ["mongo",removePOIFromPostgres],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with CH");
             	cb();

             }

             )},function(cb) {
          async.auto( {
             country: function(cb,result) {cb(null,"DE")},
             overpass: ["country",getPOIOverpass],
             mongo:["overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromPostgres],
             insert: ["mongo",insertPOIFromPostgres],
             remove: ["mongo",removePOIFromPostgres],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with AT");
             	cb();

             }

             )},

             function(cb) {
          async.auto( {
             country: function(cb,result) {cb(null,"AT")},
             overpass: ["country",getPOIOverpass],
             mongo:["overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromPostgres],
             insert: ["mongo",insertPOIFromPostgres],
             remove: ["mongo",removePOIFromPostgres],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with DE");
             	cb();

             }

             )}]);
