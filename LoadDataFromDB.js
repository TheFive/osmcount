// 
// 
// LoadDataFromDB.js
//
// This module loads static data from the database to have all relevant
// keys in memory for display.
//
// current implemented: de:regionalschluessel map
//
//

var async=require('async');
var mc = require('mongodb').MongoClient;

var config=require('./configuration');


var mongodb;
var schluesselMap = Object();
var dataLoaded = false;


exports.schluessel = function () {
	if (!dataLoaded) {
		async.series([
			function(callback){ 
				console.log("doing a");
				config.initialise();
				callback(null)
			},
			function(callback){ 
				console.log("doing b");	
				mc.connect(config.getDBString(), function(err, db) {
					if (err) throw err;
					mongodb = db;
					console.log(typeof(mongodb));
					console.log("Connected to Database mosmcount");
					callback(null);
				});	
			},
			function(callback) {
				console.log("doing c");
				console.log(typeof(mongodb));	
				mongodb.collection("OSMBoundaries").find( 
							{ boundary : "administrative" , 
							  admin_level: {$in: ['1','2','3','4','5','6','7','8','9']}
							}, 
							function(err, result) {
				if (err) console.log(err);
				result.count(function(err, count){
					console.log("Total matches: "+count);
				});
				result.each(function(err, doc) {
					if (doc == null) {
						//schleife beendet
						dataLoaded=true;
						callback(null); 
					} else {
						key=value="";
						if (doc) {
							key = doc["de:regionalschluessel"];
							value = doc.name + '('+doc.admin_level+')';
							if (typeof(key)!='undefined' && typeof(value) != 'undefined') {
								
								console.log("key "+key+" value "+value);
								schluesselMap[key]=value;
							
								while (key.charAt(key.length-1)=='0') {
									key = key.slice(0,key.length-2);
									console.log("key "+key+" value "+value);
									schluesselMap[key]=value;
								}
							}
						}
					}
				})
				console.log("schluesselmap");
				console.log(schluesselMap);
			})
				
			}],
			function(err) {
				console.log("All Done");
			}
		)
	}
	return schluesselMap;
}