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


var blaetterRegioList = exports.blaetterRegio;
var blaetterAGS_DEList = exports.blaetterAGS_DE;
var blaetterAGS_ATList = exports.blaetterAGS_AT;
var blaetterPLZ_DEList = exports.blaetterPLZ_DE;

var adminLevel_DE = {'1':'admin_level 1',
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
              
var DE_AGS = {map: exports.schluesselMapAGS,
             list: exports.blaetterAGS_DE,
             keyType : "boundary",
             keyValue : "administrative",
             secondInfoKey : "admin_level",
             secondInfoValueMap: adminLevel_DE};
             
var AT_AGS = {map : exports.schluesselMapAGS_AT,
              list: exports.blaetterAGS_DE,
              keyType: "boundary",
              keyValue: "administrative"};
              
var DE_RGS = {map  : exports.schluesselMapRegio,
              list : exports.blaetterRegio,
              keyType : "boundary",
              keyValue : "administrative",
              secondInfoKey: "admin_level",
              secondInfoValueMap:adminLevel_DE};
              
var DE_PLZ = {map  : exports.schluesselMapPLZ_DE,
              list : exports.blaetterPLZ_DE,
              keyType: "boundary",
              keyValue: ["postal_code","administrative"]};
              






var dataLoaded = false;
var blaetterDefined = false;




exports.insertValue = function insertValue(map,key,osmdoc) {
  // check Definition of key and Name
  var keyDefined =  (typeof(key)!='undefined');
  var nameDefined = (typeof(osmdoc.name) != 'undefined');
  
  // check Type of Object
  var typeCorrect = false;
  if (Array.isArray(map.keyValue)) {
  	for (var i = 0; i< map.keyValue.length;i++) {
  	  if (osmdoc[map.keyType]==map.keyValue[i]) typeCorrect = true;
  	}
  } else {
    typeCorrect = (osmdoc[map.keyType]==map.keyValue);
  }
  
  if (  keyDefined && nameDefined && typeCorrect ) {
    value = {};
    value.name = osmdoc.name;
    value.typ = "-";
    if (typeof(osmdoc[map.secondInfoKey])!= 'undefined') {
      value.typ = map.secondInfoValueMap[osmdoc[map.secondInfoKey]];
      if (typeof(value.typ) == 'undefined' ) {
        value.typ = osmdoc[map.secondInfoKey];
      } 	
    }
    map.map[key]=value;
    map.list.push(key);
    while (key.charAt(key.length-1)=='0') {
    	key = key.slice(0,key.length-1);
      map.map[key]=value;
    }
  }
}

exports.sortAndReduce = function sortAndReduce(list) { 
	list.sort();
	for (i=list.length-2;i>=0;i--)
  {
    if (list[i]==list[i+1].substring(0,list[i].length)) {
		  list.splice(i,1);
		}
	}			
}




exports.initialise = function (cb) {
  debug("initialise");
  mongodb = config.getDB();
	if (!dataLoaded) {
		async.series([
			function(callback) {
				mongodb.collection("OSMBoundaries").find( {}, 
								function(err, result) {
					if (err) {
						console.log("Error occured in function: LoadDataFromDB.initialise");
						console.log(err);
						callback(err);
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
								if (doc.osmcount_country == "DE" && doc.boundary=="postal_code") {
									keyPLZ_DE = doc["postal_code"];
								} 
								exports.insertValue(DE_RGS,keyRegio,doc);
							  exports.insertValue(DE_AGS,keyAGS,doc);
							  exports.insertValue(AT_AGS,keyAGS_AT,doc);
								exports.insertValue(DE_PLZ,keyPLZ_DE,doc);
							}
						}
					})
				})
			},
			function (callback) {
				if (!dataLoaded) {callback("No Data Loaded"); return;}
				if (blaetterDefined) {callback(null);return;}
				
				exports.sortAndReduce(blaetterRegioList);
				exports.sortAndReduce(blaetterAGS_DEList);
				exports.sortAndReduce(blaetterAGS_ATList);
				exports.sortAndReduce(blaetterPLZ_DEList);
				
				blaetterDefined=true;
				callback(null);
			}
			
			],
			function(err) {
			  if (err) {
			    console.log("exports.initialise incomplete: "+JSON.stringify(err));
			    cb(err);
			    return;
			  }
				debug("Initialising All Done");
				if (cb) cb(null);
			}
		)
	}
}

