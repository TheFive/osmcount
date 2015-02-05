var loadDataFromDB =   require('./LoadDataFromDB.js');
var configuration =   require('./configuration.js');
var async = require('async');
var debug   =  require('debug')('LoadOverpassData');
 debug.entry = require('debug')('LoadOverpassData:entry')
 debug.data = require('debug')('LoadOverpassData:data')
var wochenaufgabe = require('./wochenaufgabe.js');


// Temporary Code to Load Overpass Basic Data Claims from OpenStreetMap

// To be changed to a more readable import routine


queryBoundaries_DE='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"];rel(area)[admin_level];out;' 
queryBoundaries_AT='[out:json][timeout:900];area[type=boundary]["int_name"="Österreich"]["admin_level"="2"];rel(area)[admin_level];out;' 

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
                statusCode: response.statusCode,
                body: body
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

function loadBoundariesDE(cb,result) {
	debug.entry("loadBoundariesDE");
	overpassQuery(queryBoundaries_DE,cb);
}
function loadBoundariesAT(cb,result) {
	debug.entry("loadBoundariesAT");
	overpassQuery(queryBoundaries_AT,cb);
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
	var db = configuration.getDB();
	
	var boundariesOverpass;
	var boundaries = [];
	var b;
	
	boundariesOverpass =JSON.parse(result.overpassDE).elements;
	for (i=0;i<boundariesOverpass.length;i++) {
		b = {};
		b = boundariesOverpass[i].tags;
		b.osm_id = boundariesOverpass[i].id;
		b.osm_type = boundariesOverpass[i].type;
		boundaries.push(b);
	}	
    boundariesOverpass = JSON.parse(result.overpassAT).elements;

	for (i=0;i<boundariesOverpass.length;i++) {
		b = {};
		console.dir(boundariesOverpass[i]);
		b = boundariesOverpass[i].tags;		
		b.osm_id = boundariesOverpass[i].id;
		b.osm_type = boundariesOverpass[i].type;
		boundaries.push(b);
	}	
	
	db.collection("OSMBoundaries").insert(boundaries,{w:1},function(err,result){
		debug.entry("insertBoundariesData->CB");
		cb(err,null);
	});
}


exports.importBoundaries = function(job,cb)  {
	debug.entry("importBoundaries");
	
	// Start Overpass Query to load data
	async.auto ({ overpassDE:    loadBoundariesDE,
				  overpassAT:  ["overpassDE", loadBoundariesAT],
				  removeData:  ["overpassAT","overpassDE",removeBoundariesData],
				  insertData:  ["removeData", insertBoundariesData ] },
				  function(err,result) {
				    debug.entry("importBoundaries-> CB");
				  	if (err) {
				  		job.error = err;
				  		job.status="error";
				  		
				  	}
				  	else {
				  		console.log("Loading Boundaries Done");
				  		job.status="done";
				  	}
				  	if (cb) cb();
				  });
				   
}



exports.runOverpass= function(query, job,result, cb) {
	debug.entry("runOverpass(query,"+measure+",result,cb)");
	var measure=job.measure;
	var overpassStartTime = new Date().getTime();
	overpassQuery(query,function(error, data) {
		debug.entry("runOverpass->CB(");	
		var overpassEndTime = new Date().getTime();
		var overpassTime = overpassEndTime - overpassStartTime;
		job.overpassTime = overpassTime;
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
			if ((measure == "Apotheke")||(measure == "Apotheke_AT")) {
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
			//debug.data("Result"+JSON.stringify(result));
			cb();
		}
	}
)}



exports.createQuery = function(aufgabe,exectime,referenceJob)
{
	debug.entry("createQuery("+aufgabe+","+exectime+","+referenceJob+")");
	var jobs = [];
 	var blaetter;
 	if (aufgabe == "AddrWOstreet") {
 		blaetter = loadDataFromDB.blaetterRegio;
 	}
 	if (aufgabe == "Apotheke") {
 		blaetter = loadDataFromDB.blaetterAGS_DE;
 	}
 	if (aufgabe == "Apotheke_AT") {
 		blaetter = loadDataFromDB.blaetterAGS_AT;
 	}
	if ((aufgabe == "AddrWOStreet") || (aufgabe == "Apotheke")|| (aufgabe == "Apotheke_AT")) {
		keys = blaetter;
		for (i =0;i<keys.length;i++) {	
			debug(keys[i]);	
			var job = {};
			job.measure=aufgabe;
			job.schluessel=keys[i];
			job.status='open';
			job.exectime = exectime;
			job.type = "overpass";
			job.query = wochenaufgabe.map[aufgabe].overpass.query.replace(':schluessel:',job.schluessel);
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
   