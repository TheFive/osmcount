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



exports.schluesselMapRegio = Object();
exports.blaetterRegio = [];
exports.schluesselMapAGS = Object();
exports.blaetterAGS = Object();

blaetterRegioList = schluesselMapAGS.blaetter;
blaetterAGSList = blaetterAGS.blaetter;


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
								keyRegio = doc["de:regionalschluessel"];
								keyAGS = doc["de.amtlicher_gemeindeschluessel"]
								value = {};
								value.name = doc.name ;
								if (typeof(doc.admin_level)!= 'undefined') {
									value.typ = adminLevel[doc.admin_level];
								}
								else value.typ = "-";
								if (typeof(keyRegio)!='undefined' && typeof(value.name) != 'undefined') {
									exports.schluesselMapRegio[keyRegio]=value;
									blaetterRegioList.push(key);
							
									while (keyRegio.charAt(keyRegio.length-1)=='0') {
										keyRegio = keyRegio.slice(0,key.length-1);
										
										exports.schluesselMapRegio[keyRegio]=value;
									}
								}
								if (typeof(keyAGS)!='undefined' && typeof(value.name) != 'undefined') {
									exports.schluesselMapAGS[keyAGS]=value;
									blaetterAGSList.push(keyAGS);
							
									while (keyAGS.charAt(keyAGS.length-1)=='0') {
										keyAGS = key.slice(0,keyAGS.length-1);
										
										exports.schluesselMapAGS[keyAGS]=value;
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
				blaetterRegioList.sort();
				
				
				for (i=blaetterRegioList.length-2;i>=0;i--)
				{
					if (blaetterRegioList[i]==blaetterRegioList[i+1].substring(0,blaetterRegioList[i].length)) {
						blaetterRegioList.splice(i,1);
					}
				}
				
				blaetterAGSList.sort();
				
				
				for (i=blaetterAGSList.length-2;i>=0;i--)
				{
					if (blaetterAGSList[i]==blaetterAGSList[i+1].substring(0,blaetterAGSList[i].length)) {
						blaetterAGSList.splice(i,1);
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

