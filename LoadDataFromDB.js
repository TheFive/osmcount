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
exports.schluesselMapAGS_AT = Object();
exports.schluesselMapPLZ_DE = Object();
exports.blaetterAGS_DE = [];
exports.blaetterAGS_AT = [];
exports.blaetterPLZ_DE = [];


blaetterRegioList = exports.blaetterRegio;
blaetterAGS_DEList = exports.blaetterAGS_DE;
blaetterAGS_ATList = exports.blaetterAGS_AT;
blaetterPLZ_DEList = exports.blaetterPLZ_DE;


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
              
adminLevel_AT = {};


exports.initialise = function (cb) {
    debug("initialise");
	mongodb = config.getDB();
	if (!dataLoaded) {
		async.series([
			function(callback) {
				mongodb.collection("OSMBoundaries").find( 
							{ }
							, 
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
								var keyRegio = doc["de:regionalschluessel"];
								var keyAGS = doc["de:amtlicher_gemeindeschluessel"]
								var keyAGS_AT = doc ["ref:at:gkz"];
								var keyPLZ_DE;
								if (doc.osmcount_country == "DE" && doc.boundary="postal_code") {
									keyPLZ_DE = doc["postal_code"];
								} 
								var value = {};
								value.name = doc.name ;
								value.typ = "-";
								if (typeof(keyRegio)!='undefined' && typeof(value.name) != 'undefined') {
									if (typeof(doc.admin_level)!= 'undefined') {
										value.typ = adminLevel[doc.admin_level];
									}
									exports.schluesselMapRegio[keyRegio]=value;
									blaetterRegioList.push(keyRegio);
							
									while (keyRegio.charAt(keyRegio.length-1)=='0') {
										keyRegio = keyRegio.slice(0,key.length-1);
										
										exports.schluesselMapRegio[keyRegio]=value;
									}
								}
								if (typeof(keyAGS)!='undefined' && typeof(value.name) != 'undefined') {
									if (typeof(doc.admin_level)!= 'undefined') {
										value.typ = adminLevel[doc.admin_level];
									}
									exports.schluesselMapAGS[keyAGS]=value;
									blaetterAGS_DEList.push(keyAGS);
							
									while (keyAGS.charAt(keyAGS.length-1)=='0') {
										keyAGS = keyAGS.slice(0,keyAGS.length-1);
										exports.schluesselMapAGS[keyAGS]=value;
									}
								}
								if (typeof(keyAGS_AT)!='undefined' && typeof(value.name) != 'undefined') {
									if (typeof(doc.admin_level)!= 'undefined') {
										value.typ = adminLevel_AT[doc.admin_level];
										if(typeof(value.typ) == 'undefined') {
											value.typ = "";
										}
									}
									exports.schluesselMapAGS_AT[keyAGS_AT]=value;
									blaetterAGS_ATList.push(keyAGS_AT);
							
									while (keyAGS_AT.charAt(keyAGS_AT.length-1)=='0') {
										keyAGS_AT = keyAGS_AT.slice(0,keyAGS_AT.length-1);
										exports.schluesselMapAGS_AT[keyAGS_AT]=value;
									}
								}
								if (typeof(keyPLZ_DE)!='undefined' ) {
									var name = doc.note;
									if (typeof(name)== 'undefined') {
										name = doc.name;
									}
									if ( name && name.length >40) {
									   name = name.substr(0,40);
									   name += "...";
									}
								    var value = {};
								    value.name = name ;
								    value.typ = "-";
									
									exports.schluesselMapPLZ_DE[keyPLZ_DE]=value;
									blaetterPLZ_DEList.push(keyPLZ_DE);
							
								}
							}
						}
					})
				})
			},
			function (err,result) {
				if (!dataLoaded) {cb(null); return;}
				if (blaetterDefined) {cb(null);return;}

				blaetterRegioList.sort();
				
				
				for (i=blaetterRegioList.length-2;i>=0;i--)
				{
					if (blaetterRegioList[i]==blaetterRegioList[i+1].substring(0,blaetterRegioList[i].length)) {
						blaetterRegioList.splice(i,1);
					}
				}
				
				blaetterAGS_DEList.sort();
				
				
				for (i=blaetterAGS_DEList.length-2;i>=0;i--)
				{
					if (blaetterAGS_DEList[i]==blaetterAGS_DEList[i+1].substring(0,blaetterAGS_DEList[i].length)) {
						blaetterAGS_DEList.splice(i,1);
					}
				}
				blaetterAGS_ATList.sort();
				
				
				for (i=blaetterAGS_ATList.length-2;i>=0;i--)
				{
					if (blaetterAGS_ATList[i]==blaetterAGS_ATList[i+1].substring(0,blaetterAGS_ATList[i].length)) {
						blaetterAGS_ATList.splice(i,1);
					}
				}
				
				blaetterDefined=true;
				if (cb) cb(null);
			}
			
			],
			function(err) {
				debug("Initialising All Done");
				if (cb) cb(null);
			}
		)
	}
}

