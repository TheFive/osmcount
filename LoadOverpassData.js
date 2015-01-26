var loadDataFromDB =   require('./LoadDataFromDB.js');
var configuration =   require('./configuration.js');
var async = require('async');
var debug   =  require('debug')('LoadOverpassData');
 debug.entry = require('debug')('LoadOverpassData:entry')
 debug.data = require('debug')('LoadOverpassData:data')


// Temporary Code to Load Overpass Basic Data Claims from OpenStreetMap

// To be changed to a more readable import routine


queryBoundaries='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"];rel(area)[admin_level];out;' 

var overpassApiLinkRU = "http://overpass.osm.rambler.ru/cgi/interpreter ";
var overpassApiLinkDE = "http://overpass-api.de/api/interpreter";



var request = require('request');

function overpassQuery(query, cb, options) {
	debug.entry("overpassQuery");
	
    options = options || {};
    request.post(options.overpassUrl || overpassApiLinkDE, function (error, response, body) {
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

exports.overpassQuery = overpassQuery;

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



exports.runOverpass= function(query, job,result, cb) {
	debug.entry("runOverpass(query,"+measure+",result,cb)");
	measure=job.measure;
	overpassQuery(query,function(error, data) {
		debug.entry("runOverpass->CB(");	
		if (error) {
			console.log("Error occured in function: LoadOverpassData.runOverpass");
			console.log(error);
			cb(error);
		} else {
			date = new Date();
			result.timestamp=job.exectime;
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


exports.runOverpassPOI= function(query, job,result, cb) {
	debug.entry("runOverpassPOI(%s,job,result,cb)",query);
	overpassQuery(query,function(error, data) {
		debug.entry("runOverpassPOI->CB(");	
		if (error) {
			console.log("Error occured in function: LoadOverpassData.runOverpass");
			console.log(error);
			cb(error);
		} else {
			data=JSON.parse(data).elements;
			timestamp = data.osm3s.timestamp_osm_base;
			
			
			mongodb.collection("xxxxx").insert(jobs,
			function (err, records) {
				if (err) {
					console.log("Error occured in function: QueueWorker.doInsertJobs");
					console.log(err);
					job.status="error";
					job.error = err;
					err=null; //error wird gespeichert, kann daher hier auf NULL gesetzt werden.
				} else {
					debug("All Inserted %i" ,records.length);
					job.status='done';
				}					
				cb(err,job);
			})
			
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
			job.query = wochenaufgabe.map[aufgabe].query.replace(':schluessel:',job.schluessel);
			job.query = job.query.replace(':timestamp:',exectime.toISOString());
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
   