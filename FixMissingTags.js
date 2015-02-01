var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('test');
  debug.data = require('debug')('test:data');
  debug.entry = require('debug')('test:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js');
var request = require('request');
var ObjectID = require('mongodb').ObjectID;


var object=ObjectID("54bbda23a9fff29864d116b4");

var async    = require('async');

function readError(cb,result) {


configuration.getDB().collection("DataCollection").find( 
							      {"missing.name":{$exists:0},measure:"Apotheke"}).each(
								function(err,result) {
  if (err) {
    console.log("Error: "+JSON.stringify(err));
    return;
  }
  if (result== null) return;
  //console.log("Vorher: Result");
  //console.dir(result);
  result.count=0;
  result.count = result.data.length;
	if (result.measure == "Apotheke") {
		result.missing = {};
		result.existing = {}
		result.existing.fixme = 0;
		result.missing.opening_hours = 0;
		result.missing.phone=0;
		result.missing.wheelchair = 0;
		result.missing.name = 0;
	
		for (i = 0 ; i< result.data.length;i++ ) {
			p = result.data[i].tags;
			if (typeof(p) == 'undefined') continue;
			//console.log("Zähle jetzt");
			//console.dir(p);
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
  //console.log("Nacher: Result");
  //console.dir(result);
   console.log("saved call");
  configuration.getDB().collection("DataCollection").save(result,{w:1},function(err,r){
   console.log("saved called");
  	if (err) {
  		console.log("error during Save");
  		console.dir(err);
  		
  	} else {
  		console.log("Updated: "+r);
  	}
  	});
	
	
								
});}

async.auto( {db:configuration.initialiseDB,
	         error:["db",readError]},
	         function (cb,result) {
	         	console.dir(result.error);
	         	cb();
	         }
             
             );