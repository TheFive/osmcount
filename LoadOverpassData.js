var loadDataFromDB =   require('./LoadDataFromDB.js');
var configuration =   require('./configuration.js');
var async = require('async');
var debug   =  require('debug')('LoadOverpassData');
 debug.entry = require('debug')('LoadOverpassData:entry')
 debug.data = require('debug')('LoadOverpassData:data')


// Temporary Code to Load Overpass Basic Data Claims from OpenStreetMap

// To be changed to a more readable import routine

exports.query = {
   Apotheke:'[out:json];area["de:amtlicher_gemeindeschluessel"="######"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
   ApothekeDetail:'[out:json];area["de:amtlicher_gemeindeschluessel"="######"]->.a;\n(node(area.a)[amenity=pharmacy][$$$$];\nway(area.a)[amenity=pharmacy][$$$$];\nrel(area.a)[amenity=pharmacy][$$$$]);\nout;',
   AddrWOStreet: '[out:json][timeout:900];area[type=boundary]["de:regionalschluessel"="######"]->.boundaryarea;\n\
rel(area.boundaryarea)[type=associatedStreet]->.associatedStreet; \n\
\n\
way(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesWay; \n\
way(r.associatedStreet:"house")->.asHouseWay; \n\
(.allHousesWay; - .asHouseWay); out ids; \n\
 \n\
node(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesNode; \n\
node(r.associatedStreet:"house")->.asHouseNode; \n\
(.allHousesNode; - .asHouseNode);out ids; \n\
 \n\
rel(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesRel; \n\
rel(r.associatedStreet:"house")->.asHouseRel; \n\
(.allHousesRel; - .asHouseRel);out ids;'
}

queryBoundaries='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"];rel(area)[admin_level];out;' 



var request = require('request');

function overpassQuery(query, cb, options) {
	debug.entry("overpassQuery");
    options = options || {};
    request.post(options.overpassUrl || 'http://overpass-api.de/api/interpreter', function (error, response, body) {
    	debug.entry("overpassQuery->CB");

        if (!error && response.statusCode === 200) {
            cb(undefined, body);
        } else if (error) {
        	console.log("Error occured in function: LoadOverpassData.overpassQuery");
        	console.log(error);
        	console.log(body);
            cb(error);
        } else if (response) {
            cb({
                message: 'Request failed: HTTP ' + response.statusCode,
                statusCode: response.statusCode
            });
        } else {
            cb({
                message: 'Unknown error.',
            });
        }
    }).form({
        data: query
    });
};

function loadBoundaries(cb,result) {
	debug.entry("loadBoundaries");
	overpassQuery(queryBoundaries,cb);
}
function removeBoundariesData (cb,result) {
	debug.entry("removeBoundariesData");
	db = configuration.getDB();
	db.collection("OSMBoundaries").remove({},{w:1}, function (err,count) {
		debug.entry("removeBoundariesData->CB count = "+count);
		cb(err,count);
	})
}
function insertBoundariesData (cb,result) {
	debug.entry("insertBoundariesData");
	db = configuration.getDB();
	
	boundariesOverpass = JSON.parse(result.overpass).elements;
	boundaries = [];
	for (i=0;i<boundariesOverpass.length;i++) {
		boundaries[i]=boundariesOverpass[i].tags;
		boundaries[i].osm_id = boundariesOverpass[i].id;
		boundaries[i].osm_type = boundariesOverpass[i].type;
	}	
	
	db.collection("OSMBoundaries").insert(boundaries,{w:1},function(err,result){
		debug.entry("insertBoundariesData->CB");
		cb(err,null);
	});
}


exports.importBoundaries = function(job,cb)  {
	debug.entry("importBoundaries(cb)");
	
	// Start Overpass Query to load data
	async.auto ({ overpass:    loadBoundaries,
				  removeData:  ["overpass",removeBoundariesData],
				  insertData:  ["removeData", insertBoundariesData ] },
				  function(err,result) {
				  	if (err) {
				  		job.error = err;
				  		job.status="error";
				  		
				  	}
				  	else {
				  		job.status="done";
				  	}
				  	if (cb) cb();
				  });
				   
}



exports.runOverpass= function(query, measure,result, cb) {
	debug.entry("runOverpass(query,"+measure+",result,cb)");
	overpassQuery(query,function(error, data) {
		debug.entry("runOverpass->CB(");	
		if (error) {
			console.log("Error occured in function: LoadOverpassData.runOverpass");
			console.log(error);
			cb(error);
		} else {
			date = new Date();
			result.timestamp=date;
			result.count=0;
			result.data=JSON.parse(data).elements;
			result.measure=measure;
			result.count = result.data.length;
			if (measure == "Apotheke") {
				result.missing = {};
				result.existing = {}
				result.existing.fixme = 0;
				result.missing.opening_hours = 0;
				result.missing.phone=0;
				result.missing.wheelchair = 0;
				result.missing.name = 0;
				for (i = 0 ; i< result.data.length;i++ ) {
					p = result.data[i].tags;
					if (!p.hasOwnProperty("opening_hours")) {
						result.missing.opening_hours += 1;
					}
					if (!p.hasOwnProperty("name")) {
						result.missing.name += 1;
					}
					if (!p.hasOwnProperty("wheelchair")) {
						result.missing.wheelchair += 1;
					}
					if (p.hasOwnProperty("fixme")) {
						result.existing.fixme += 1;
					}
					if (!p.hasOwnProperty("phone") && ! p.hasOwnProperty("contact:phone")) {
						result.missing.phone += 1;
					}
				}
			}
			debug.data("Result"+JSON.stringify(result));
			cb();
		}
	}
)}

exports.createQuery = function(aufgabe,exectime,referenceJob)
{
	debug.entry("createQuery("+aufgabe+","+exectime+","+referenceJob+")");
	jobs = [];
 	var blaetter;
 	if (aufgabe == "AddrWOstreet") {
 		blaetter = loadDataFromDB.blaetterRegio;
 	}
 	if (aufgabe == "Apotheke") {
 		blaetter = loadDataFromDB.blaetterAGS;
 	}
	if ((aufgabe == "AddrWOStreet") || (aufgabe == "Apotheke")) {
		keys = blaetter;
		for (i =0;i<keys.length;i++) {	
			debug(keys[i]);	
			job = {};
			job.measure=aufgabe;
			job.schluessel=keys[i];
			job.status='open';
			job.exectime = exectime;
			job.type = "overpass";
			job.query = exports.query[aufgabe].replace('######',job.schluessel);
			job.source = referenceJob._id;
			jobs.push(job);
			
		}
		return jobs;
	}
	return [];
}

function readOverpassBoundaries(cb, results) {

}

exports.getBoundariesDumpFromOverpass = function(callback) {

	async.auto ({
		readOverpass: readOverpassBoundaries,
		removeBoundaries : ["readOverpass",removeBoundaries],
		insertBoundaries : ["removeBoundaries",insertBoundaries]
		}, function (err, result) {
			if (err) {
				console.log(err);
				if (callback) callback();
			}
			
		})

}
   