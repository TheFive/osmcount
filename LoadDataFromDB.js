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
var blaetterList = [];
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
				mongodb.collection("OSMBoundaries").find( 
							{ boundary : "administrative" , 
							  admin_level: {$in: ['1','2','3','4','5','6','7','8','9']}
							}, 
								function(err, result) {
					if (err) console.log(err);
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
									blaetterList.push(key);
							
									while (key.charAt(key.length-1)=='0') {
										key = key.slice(0,key.length-2);
										console.log("key "+key+" value "+value);
										schluesselMap[key]=value;
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
				console.log("Länge nach Sort: "+blaetterList.length);
				
				for (i=blaetterList.length-2;i>=0;i--)
				{
					if (blaetterList[i]==blaetterList[i+1].substring(0,blaetterList[i].length)) {
						blaetterList.splice(i,1);
					}
				}
				console.log("Länge nach Bereinigung: "+blaetterList.length);
				blaetterDefined=true;
				callback(null);
			}
			
			],
			function(err) {
				console.log("All Done");
			}
		)
	}
	return schluesselMap;
}