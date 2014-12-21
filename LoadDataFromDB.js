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
var debug   = require('debug')('LoadDataFromDB');



exports.schluesselMap = Object();
exports.blaetter = [];
blaetterList = exports.blaetter;


var dataLoaded = false;
var blaetterDefined = false;

adminLevel = {'1':'admin_level 1',
              '2': 'Staat',
              '3': 'admin_level 3',
              '4': 'Bundesland',
              '5': 'Regierungsbezirk',
              '6': 'Kreis',
              '7': 'Ort (7)',
              '8': 'Ort (8)',
              '9': 'Ortsteil (9)',
              '10': 'Ortsteil (10)',
              '11': 'Ortsteil (11)'        }


exports.initialise = function (cb) {
	mongodb = config.getDB();
	if (!dataLoaded) {
		async.series([
			function(callback) {
				mongodb.collection("OSMBoundaries").find( 
							{ boundary : "administrative" , 
							  admin_level: {$in: ['1','2','3','4','5','6','7','8','9']}
							}, 
								function(err, result) {
					if (err) {
						console.log("Error occured in function: LoadDataFromDB.initialise");
						console.log(err);
					}
					result.each(function(err, doc) {
						if (doc == null) {
							//schleife beendet
							dataLoaded=true;
							callback(null); 
						} else {
							key=value="";
							if (doc) {
								key = doc["de:regionalschluessel"];
								value = {};
								value.name = doc.name ;
								if (typeof(doc.admin_level)!= 'undefined') {
									value.typ = adminLevel[doc.admin_level];
								}
								else value.typ = "-";
								if (typeof(key)!='undefined' && typeof(value.name) != 'undefined') {
									exports.schluesselMap[key]=value;
									blaetterList.push(key);
							
									while (key.charAt(key.length-1)=='0') {
										key = key.slice(0,key.length-1);
										
										exports.schluesselMap[key]=value;
									}
								}
							}
						}
					})
				})
			},
			function (callback) {
				if (!dataLoaded) {callback(null); return;}
				if (blaetterDefined) {callback(null);return;}
				blaetterList.sort();
				
				
				for (i=blaetterList.length-2;i>=0;i--)
				{
					if (blaetterList[i]==blaetterList[i+1].substring(0,blaetterList[i].length)) {
						blaetterList.splice(i,1);
					}
				}
				
				blaetterDefined=true;
				callback(null);
			}
			
			],
			function(err) {
				debug("Initialising All Done");
				cb();
			}
		)
	}
}

