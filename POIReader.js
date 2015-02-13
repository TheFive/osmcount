var debug    = require('debug')('POIReader');

var fs=require("fs");
var path    = require('path');
var config = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js');
var request = require('request');


var async    = require('async');




var query = 
{ DE:'[timeout:4800][out:json];area["int_name"="Deutschland"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;',
 AT: '[timeout:1800][out:json];area["int_name"="Österreich"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;',
 CH: '[timeout:1800][out:json];area["int_name"="Schweiz"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;'
}

function cleanObject(obj) {
	
     for (k in obj) {
     	if (typeof (obj[k]) =='object') {
     	  cleanObject(obj[k]);
     	}
     	if (k.indexOf(".") >0 ) {
     	    //console.log("Fount ." +k.search("."));
     		//console.dir(data);
     		
     		delete obj[k]; 
     		console.log("Deletet"+k);
     		//console.log("After Delete");
     		//console.dir(data);
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
		var result = fs.readFileSync(filename);
		data = JSON.parse(result);
		debug("Loaded Elemnts:"+data.elements.length);
		cleanTags(data.elements);
		cb(null, data);
		return;
	}
	debug("Overpass Abfrage Starten für "+country);
	
	loadOverpassData.overpassQuery(query[country], function(err,result) {
		debug("getPOIByPLZOverpass->CB");
		debug("Overpass Abfrage Beendet für "+country);
		if (err) {
			console.log("Fehler: "+JSON.stringify(err));
			console.log(JSON.stringify(result));
			cb(err,null);
		} else {
			fs.writeFileSync(filename,result);
			data = JSON.parse(result);
			debug("Loaded Elements:"+data.elements.length);
     		cleanTags(data.elements);
			cb(null,data);
		}
	})
}


function getPOIByPLZMongo(cb,result) {
	debug("getPOIByPLZMongo");
	
	var country = result.country;
	var db = config.getDB();
	var data = result.overpass;
	list = {};
	debug("Elemente geladen: "+data.elements.length+" für "+country);
	
	
	for (i =0;i<data.elements.length;i++) {
		element = data.elements[i];
		keyIntern = element.type + element.id;
		element.overpass = {};
		element.overpass["loadBy"] = country;
		list[keyIntern] = element;
	}
	var mongoQuery = { "overpass.loadBy":country, "tags.amenity": "pharmacy"}

	db.collection("POI").find(mongoQuery).toArray( function(err,result) {
	    debug("getPOIByPLZMongo->CB");
		if (err) {
			console.log("Fehler "+err);
			cb(err,null);
		} else {
			var remove = [];
			var update = [];
			var insert = [];
			debug("Found "+result.length+ " POIs in DB");
			for (i=0;i<result.length;i++ ) {
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
			for (k in list) {
				if (typeof(list[k]._id) == 'undefined') {
					insert.push(list[k]);
				}
			}		
			erg = {};
			debug("To be removed: "+remove.length);
			debug("To be updated: "+update.length);
			debug("To be inserted: "+insert.length);
			erg.remove = remove;
			erg.update = update;
			erg.insert = insert;
			cb(null,erg);
		}			
	})
}

function removePOIFromMongo(cb,result) {
  debug("removePOIFromMongo");
  var remove = result.mongo.remove;
  if (remove.length==0) {
  	cb(null,null);
  	return;
  }
  debug("To Be Removed: "+remove.length + " DataSets");
  var db = config.getDB();
  
  var q = async.queue(function (task, cb) {
    debug("removePOIFromMongo->Queue");
    db.collection("POI").remove({_id:task.data._id}, function(err,result) {
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


function updatePOIFromMongo(cb,result) {
  debug("updatePOIFromMongo");
  var update = result.mongo.update;
  if (update.length==0) {
  	cb(null,null);
  	return;
  }
  cleanTags(update);
  debug("To Be Updated: "+update.length + " DataSets");
  db = config.getDB();

  var q = async.queue(function (task, cb) {
    debug("updatePOIFromMongo->Queue");
   
    db.collection("POI").save(task.data, function(err,result) {
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

function insertPOIFromMongo(cb,result) {
  debug("insertPOIFromMongo");
  var insert = result.mongo.insert;
  debug("To Be Inserted: "+insert.length + " DataSets");
  db = config.getDB();
  if (insert.length == 0) {
  	cb(null,null);
  	return;
  }
  db.collection("POI").insert(insert, function(err,result) {
    debug("insertPOIFromMongo->CB");
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
    db = config.getDB();
    db.collection("POI").findOne( 
                           {$or:[
                           {"nominatim.timestamp":{$exists:0}},
                           { "nominatim.timestamp":{$gte:"$timestamp"}}
                             ]}, function(err, obj) {
      debug("nominatim->CB");
      if (err) {
        console.log("Error occured in function: QueueWorker.getNextJob");
        console.log(err);
        cb(err,null);
        return;
      }
      if (obj) {
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
			//console.dir(elementData);
			cleanObject(obj);
			db.collection("POI").save(obj, function(err,result) {
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
  
  async.series([ function(cb) {
       async.auto( {config: config.initialiseDB,
             country: function(cb,result) {cb(null,"CH")},
             overpass: ["country",getPOIOverpass],
             mongo:["config","overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromMongo],
             insert: ["mongo",insertPOIFromMongo],
             remove: ["mongo",removePOIFromMongo],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with CH");
             	cb();
             	
             }
             
             )},function(cb) {
          async.auto( {config: config.initialiseDB,
             country: function(cb,result) {cb(null,"DE")},
             overpass: ["country",getPOIOverpass],
             mongo:["config","overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromMongo],
             insert: ["mongo",insertPOIFromMongo],
             remove: ["mongo",removePOIFromMongo],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with AT");
             	cb();
             	
             }
             
             )},
             
             function(cb) {
          async.auto( {config: config.initialiseDB,
             country: function(cb,result) {cb(null,"AT")},
             overpass: ["country",getPOIOverpass],
             mongo:["config","overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromMongo],
             insert: ["mongo",insertPOIFromMongo],
             remove: ["mongo",removePOIFromMongo],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	debug("READY with DE");
             	cb();
             	
             }
             
             )}]);
