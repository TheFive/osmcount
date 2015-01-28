var debug    = require('debug')('POIReader');
  debug.data = require('debug')('POIReader:data');
  debug.entry = require('debug')('POIReader:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js');
var request = require('request');


var async    = require('async');




query = '[out:json];area["int_name"="Deutschland"]["admin_level"="2"]->.a;\
(node(area.a)[amenity=pharmacy]; \
 way(area.a)[amenity=pharmacy]; \
 rel(area.a)[amenity=pharmacy]); \
out center meta;';




function getPOIByPLZOverpass(cb,result) {
	debug.entry("getPOIByPLZOverpass");
	console.log("Overpass Abfrage Starten");
	loadOverpassData.overpassQuery(query, function(err,result) {
		debug.entry("getPOIByPLZOverpass->CB");
		console.log("Overpass Abfrage Beendet");
		if (err) {
			console.log("Fehler: "+err);
			cb(err,null);
		} else {
			data = JSON.parse(result);
			cb(null,data);
		}
	})
}


function getPOIByPLZMongo(cb,result) {
	debug.entry("getPOIByPLZMongo");
	
	mongodb = configuration.getDB();
	data = result.overpass.data;
	key = result.overpass.key;
	list = {};
	
	for (i =0;i<data.length;i++) {
		element = data[i];
		keyIntern = element.type + element.id;
		element.overpass = {};
		element.overpass["de:amtlicher_gemeindeschluessel"] = key;
		list[keyIntern] = element;
	}
	mongoQuery = { "de:amtlicher_gemeindeschluessel":key, "tags.amenity": "pharmacy"}

	mongodb.collection("POI").find(mongoQuery).toArray( function(err,result) {
	    debug.entry("getPOIByPLZMongo->CB");
		if (err) {
			console.log("Fehler "+err);
			cb(err,null);
		} else {
			remove = [];
			update = [];
			insert = [];
			debug.data("Found "+result.length+ " POIs in DB");
			for (i=0;i<result.length;i++ ) {
				element = result[i];
				key = element.type + element.id;
				if (typeof(list[key])=='undefined') {
					remove.push(element);
				} else {
					list[key]._id = element._id;
					if (list[key].version != element.version) {
						update.push(list[key]);
					}
				}
			}
			for (k in list) {
				//console.log(k);
				if (typeof(list[k]._id) == 'undefined') {
					insert.push(list[k]);
				}
			}		
			erg = {};
			erg.remove = remove;
			erg.update = update;
			erg.insert = insert;
			cb(null,erg);
		}			
	})
}

function removePOIFromMongo(cb,result) {
  debug.entry("removePOIFromMongo");
  remove = result.mongo.remove;
  if (remove.length==0) {
  	cb(null,null);
  	return;
  }
  debug.data("To Be Removed: "+remove.length + " DataSets");
  db = configuration.getDB();
  
  var q = async.queue(function (task, cb) {
    debug.entry("removePOIFromMongo->Queue");
    mongodb.collection("POI").remove({_id:task.data._id}, function(err,result) {
      debug.entry("removePOIFromMongo->MongoCB");    
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
  debug.entry("updatePOIFromMongo");
  update = result.mongo.update;
  if (update.length==0) {
  	cb(null,null);
  	return;
  }
  debug.data("To Be Updated: "+update.length + " DataSets");
  db = configuration.getDB();

  var q = async.queue(function (task, cb) {
    debug.entry("updatePOIFromMongo->Queue");
    mongodb.collection("POI").save(task.data, function(err,result) {
      debug.entry("updatePOIFromMongo->MongoCB");    
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
  debug.entry("insertPOIFromMongo");
  insert = result.mongo.insert;
  debug.data("To Be Inserted: "+update.length + " DataSets");
  db = configuration.getDB();
  if (insert.length == 0) {
  	cb(null,null);
  	return;
  }
  db.collection("POI").insert(insert, function(err,result) {
    debug.entry("insertPOIFromMongo->CB");
    if (err) {
    	console.log("Error "+ err);
    	cb(err,null);
    } else {
    	cb (null,null);
    }
  })
}





function nominatim(cb,result) {
  debug.entry("nominatim");
  q = async.queue(function(task,cb) {
    db = configuration.getDB();
    db.collection("POI").findOne( 
                           {$or:[
                           {"nominatim.timestamp":{$exists:0}},
                           { "nominatim.timestamp":{$gte:"$timestamp"}}
                             ]}, function(err, obj) {
      debug.entry("nominatim->CB("+err+","+obj+")");
      if (err) {
        console.log("Error occured in function: QueueWorker.getNextJob");
        console.log(err);
        cb(err,null);
        return;
      }
      if (obj) {
        debug.data("found job %s",obj.type);
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
		url = "http://nominatim.openstreetmap.org/reverse?format=json&";
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
			//console.dir(elementData);
			obj.nominatim = elementData.address;
			obj.nominatim.timestamp = date;
			db.collection("POI").save(obj, function(err,result) {
			  debug.entry("nominatim->MongoCB");
			  if (err) {
				console.log("Error "+err);
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

exports.storePOI = function(result,schluessel,callback) {
  debug.entry("storePOI");
  async.auto( {overpass: function(cb) {
  					debug.entry("overpass Function async");
                   x = {data:result,key:schluessel};
                   cb(null,x);
                },
             mongo:["overpass",getPOIByPLZMongo],
             update: ["mongo",updatePOIFromMongo],
             insert: ["mongo",insertPOIFromMongo],
             remove: ["mongo",removePOIFromMongo],
             nominatim: ["update","insert","remove",nominatim]},
             function(err,results) {
             	callback();
             }
             
             );
}