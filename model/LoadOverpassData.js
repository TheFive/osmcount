var async     = require('async');
var debug     = require('debug')('LoadOverpassData');
var should    = require('should');
var request   = require('request');

var configuration  = require('../configuration.js');
var wochenaufgabe  = require('../wochenaufgabe.js');

var loadDataFromDB = require('./LoadDataFromDB.js');

// Temporary Code to Load Overpass Basic Data Claims from OpenStreetMap

// To be changed to a more readable import routine


var queryBoundaries_DE='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"]->.a;\
                     (rel(area.a)[admin_level];rel(area.a)[postal_code]);out;'
var queryBoundaries_AT='[out:json][timeout:900];area[type=boundary]["int_name"="Österreich"]["admin_level"="2"];rel(area)[admin_level];out;'
var queryBoundaries_CH='[out:json][timeout:900];area[type=boundary]["int_name"="Schweiz"]["admin_level"="2"];rel(area)[admin_level];out;'

var overpassApiLinkRU = "http://overpass.osm.rambler.ru/cgi/interpreter ";
var overpassApiLinkDE = "http://overpass-api.de/api/interpreter";

exports.timeout = 1000 * 60 * 10; // Timeout after 10 minutes;



function overpassQuery(query, cb, options) {
	debug("overpassQuery");
  options = options || {};
  if (typeof(options.uri)!= 'undefined') {
    options.uri = options.overpassUrl;
  } else {
    options.uri = overpassApiLinkDE;
  }
  if (typeof(options.timeout) == 'undefined') {
    options.timeout = exports.timeout; // Timeout after 10 minutes
  }
  var start = (new Date()).getTime();
  request.post(options, function (error, response, body) {
    var end = (new Date()).getTime();
    debug("overpassQuery->CB after %s seconds",(end-start)/1000);


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
	debug("loadBoundariesDE");
	console.log("loadBoundariesDE");
	overpassQuery(queryBoundaries_DE,cb);
}
function loadBoundariesAT(cb,result) {
	debug("loadBoundariesAT");
	console.log("loadBoundariesAT");
	overpassQuery(queryBoundaries_AT,cb);
}
function loadBoundariesCH(cb,result) {
	debug("loadBoundariesCH");
	console.log("loadBoundariesCH");
	overpassQuery(queryBoundaries_CH,cb);
}


function removeBoundariesData (cb,result) {
	debug("removeBoundariesData");
	var db = configuration.getMongoDB();
	db.collection("OSMBoundaries").remove({},{w:1}, function (err,count) {
		debug("removeBoundariesData->CB count = "+count);
		cb(err,count);
	})
}
function copyRelevantTags(boundary,osmData) {
  for (var k in wochenaufgabe.boundaryPrototype) {
    b[k]=osmData.tags[k];
  }
}

function copyBoundaries(overpassStr,countryStr,boundaries) {
	var boundariesOverpass =JSON.parse(overpassStr).elements;
	for (var i=0;i<boundariesOverpass.length;i++) {
		var b = {};
		//b = boundariesOverpass[i].tags;
		copyRelevantTags(b,boundariesOverpass[i]);
		b.osm_id = boundariesOverpass[i].id;
		b.osm_type = boundariesOverpass[i].type;
		b.osmcount_country=countryStr;
		boundaries.push(b);
	}
}

function insertBoundariesData (cb,result) {
	debug("insertBoundariesData");
	var db = configuration.getMongoDB();

	var boundaries = [];

	copyBoundaries(result.overpassCH,"CH",boundaries);
	copyBoundaries(result.overpassDE,"DE",boundaries);
	copyBoundaries(result.overpassAT,"AT",boundaries);

	db.collection("OSMBoundaries").insert(boundaries,{w:1},function(err,result){
		debug("insertBoundariesData->CB");
		cb(err,null);
	});
}


exports.importBoundaries = function(job,cb)  {
	debug("importBoundaries");

	// Start Overpass Query to load data
	async.auto ({ overpassDE:    loadBoundariesDE,
				  overpassAT:  ["overpassDE", loadBoundariesAT],
				  overpassCH:  ["overpassAT", "overpassDE", loadBoundariesCH],
				  removeData:  ["overpassAT", "overpassDE","overpassCH",removeBoundariesData],
				  insertData:  ["removeData", insertBoundariesData ] },
				  function(err,result) {
				    debug("importBoundaries-> CB");
				  	if (err) {
				  		job.error = err;
				  		job.status="error";

				  	}
				  	else {
				  		console.log("Loading Boundaries Done");
				  		job.status="done";
				  	}
				  	if (cb) cb(err);
				  });

}



exports.runOverpass= function(query, job,result, cb) {
	debug("runOverpass(query,"+measure+",result,cb)");
	var measure=job.measure;
	var overpassStartTime = new Date().getTime();
	overpassQuery(query,function(error, data) {
		debug("runOverpass->CB(");
		var overpassEndTime = new Date().getTime();
		var overpassTime = overpassEndTime - overpassStartTime;
		job.overpassTime = overpassTime;
		if (error) {
			console.log("Error occured in function: LoadOverpassData.runOverpass");
			console.log(error);
			cb(error);
		} else {
			var date = new Date();
			result.timestamp=job.exectime;
			result.count=0;
			try {
				var jsonResult=JSON.parse(data).elements;
			} catch (err) {
				console.log("Query;")
				console.log(query)
				console.log("Result");
				console.log(data);
				console.log("job");
				console.dir(job);
				cb(err);
				return;
			}
			result.measure=measure;
			result.count = jsonResult.length;

			// Berechne weitere Zahlen e.g. Missung und Existing Tags
			var tagCounter = wochenaufgabe.map[measure].tagCounter;
			if (tagCounter) tagCounter(jsonResult,result);
			cb(null);
		}
	}
)}



exports.createQuery = function(referenceJob)
{
	debug("createQuery");
	should.exist(referenceJob);
    var jobs = [];
	var aufgabe = referenceJob.measure;
	var exectime = referenceJob.exectime;
	var wa = wochenaufgabe.map[aufgabe];
	if (typeof(wa) == 'undefined')  {
	  referenceJob.error = "Wochenaufgabe nicht definiert";
	  return jobs;
	}
 	var blaetter = wa.map.list;

	if (typeof(blaetter)!='undefined') {
		var keys = blaetter;
		for (var i =0;i<keys.length;i++) {
			debug(keys[i]);
			var job = {};
			job.measure=aufgabe;
			job.schluessel=keys[i];
			should(job.schluessel).not.equal('undefined');
			job.status='open';
			job.exectime = exectime;
			job.type = "overpass";
			job.query = wochenaufgabe.map[aufgabe].overpass.query.replace(':schluessel:',job.schluessel);
			job.query = job.query.replace(':timestamp:',exectime.toISOString());
			if (typeof(referenceJob._id)!='undefined') {
			  job.source = referenceJob._id;	
			}
			if (typeof(referenceJob.id)!='undefined') {
			  job.source = referenceJob.id;	
			}
			
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
