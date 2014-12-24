var loadDataFromDB =   require('./LoadDataFromDB.js');
var debug   =  require('debug')('LoadOverpassData');
 debug.entry = require('debug')('LoadOverpassData:entry')
 debug.data = require('debug')('LoadOverpassData:data')


// Temporary Code to Load Overpass Basic Data Claims from OpenStreetMap

// To be changed to a more readable import routine

queryApotheke='area["de:regionalschluessel"="03157"];(node(area)[amenity=pharmacy];way(area)[amenity=pharmacy];rel(area)[amenity=pharmacy]);out ids;'



queryAddrWOStreet='[out:json][timeout:900];area[type=boundary]["de:regionalschluessel"="######"]->.boundaryarea; \
rel(area.boundaryarea)[type=associatedStreet]->.associatedStreet; \
 \
way(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesWay; \
way(r.associatedStreet:"house")->.asHouseWay; \
(.allHousesWay; - .asHouseWay); out ids; \
 \
node(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesNode; \
node(r.associatedStreet:"house")->.asHouseNode; \
(.allHousesNode; - .asHouseNode);out ids; \
 \
rel(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesRel; \
rel(r.associatedStreet:"house")->.asHouseRel; \
(.allHousesRel; - .asHouseRel);out ids;' 

queryBoundaries='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"];rel(area)[admin_level];out; \
' 



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


// Dubliziere die Verbindung mit der mongdb

var fs;



/*
boundariesFile = 'Boundaries OSM Nov 14.json';
fs = require('fs');

var boundariesJSON = JSON.parse(
  fs.readFileSync(boundariesFile)
);

*/




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
			length= data.length;
			for (i=0;i<length;i++) {
				if (data.charAt(i) != '"')  {continue}
				if (data.charAt(i+1) != 'i')  {continue}
				if (data.charAt(i+2) != 'd')  {continue}
				if (data.charAt(i+3) != '"')  {continue}
				if (data.charAt(i+3) != '"')  {continue}
				if (data.charAt(i+4) != ':')  {continue}
				result.count++;
			}
			debug.data("Result"+JSON.stringify(result));
			cb();
		}
	}
)}

exports.createQuery = function(aufgabe,exectime,referenceJob)
{
	jobs = [];
	if (aufgabe == "AddrWOStreet") {
		keys = loadDataFromDB.blaetter;
		for (i =0;i<keys.length;i++) {	
			debug(keys[i]);	
			job = {};
			job.measure=aufgabe;
			job.schluessel=keys[i];
			job.status='open';
			job.exectime = exectime;
			job.type = "overpass";
			job.query = queryAddrWOStreet.replace('######',job.schluessel);
			job.source = referenceJob._id;
			jobs.push(job);
			
		}
		return jobs;
	}
	return [];
}

   