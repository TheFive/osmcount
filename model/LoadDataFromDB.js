
// LoadDataFromDB.js
//
// This module loads static data from the database to have all relevant
// keys in memory for display.
//
// current implemented: de:regionalschluessel map
//
//

var async    = require('async');
var should   = require('should');
var debug    = require('debug')('LoadDataFromDB');

var config   = require('../configuration.js');
var OSMData  = require('../model/OSMData.js')





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

var AT_AGS = {map : Object(),
              list: [],
              matchKey: {boundary:"administrative",
                         osmcount_country:"AT"},
              blaetterIgnore: [{"admin_level":2}]};
var DE_RGS = {map  : Object(),
              list : [],
              matchKey: {boundary:"administrative",
                         osmcount_country:"DE"},
              secondInfoKey: "admin_level",
              secondInfoValueMap:adminLevel_DE};
var DE_PLZ = {map  : Object(),
              list : [],
              matchKey: {boundary:["postal_code","administrative"],
                         osmcount_country:"DE"}};
exports.DE_PLZ = DE_PLZ;
exports.DE_RGS = DE_RGS;
exports.AT_AGS = AT_AGS;
exports.DE_AGS = {map: Object(),
             list: [],
             matchKey: {boundary:"administrative",
                        osmcount_country:"DE"},
             secondInfoKey : "admin_level",
             secondInfoValueMap: adminLevel_DE};

var DE_AGS = exports.DE_AGS;





var dataLoaded = false;
var blaetterDefined = false;




exports.insertValue = function insertValue(map,key,osmdoc) {
  debug('insertValue');
  debug('key:%s',key);
  debug(JSON.stringify(osmdoc));
  // check Definition of key and Name
  var keyDefined =  (typeof(key)!='undefined');
  var nameDefined = (typeof(osmdoc.name) != 'undefined');

  // check Type of Object
  var allTypeCorrect = true;
  for (var k in map.matchKey) {
    var v = map.matchKey[k];
    var typeCorrect = false;
    if (Array.isArray(v)) {
  	  for (var i = 0; i< v.length;i++) {
  	    if (osmdoc[k]==v[i]) typeCorrect = true;
  	  }
    } else {
      typeCorrect = (osmdoc[k]==v);
    }
    allTypeCorrect = (allTypeCorrect && typeCorrect);
  }

  if (  keyDefined && nameDefined && allTypeCorrect ) {
    var value = {};
    value.name = osmdoc.name;
    value.typ = "-";
    if (typeof(osmdoc[map.secondInfoKey])!= 'undefined') {
      value.typ = map.secondInfoValueMap[osmdoc[map.secondInfoKey]];
      if (typeof(value.typ) == 'undefined' ) {
        value.typ = osmdoc[map.secondInfoKey];
      }
    }
    var pushOnList = true;
    // check wether to ignore in List
    if (typeof (map.blaetterIgnore)!='undefined') {
      for (var i = 0;i<map.blaetterIgnore.length;i++) {
        for (var k in map.blaetterIgnore[i]) {
          if (osmdoc[k]==map.blaetterIgnore[i][k]) {
            pushOnList = false;;
          }
        }
      }
    }
    debug('map.map[key]=value; --> key: %s, Value: %s',key,value);
    map.map[key]=value;
    should(key).not.equal('undefined');
    if (pushOnList) map.list.push(key);
    while (key.length>1 && key.charAt(key.length-1)=='0') {
    	key = key.slice(0,key.length-1);
      if (pushOnList) map.map[key]=value;
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
	if (!dataLoaded) {
		async.series([
			function(callback) {
        OSMData.find( {},
								function(err, result) {
					if (err) {
						console.log("Error occured in function: LoadDataFromDB.initialise");
						console.log(err);
						callback(err);
					}
          debug('Loaded from Database %s rows',result.length);
          for (var i = 0;i<result.length;i++) {
            doc = result[i];
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
          dataLoaded = true;
          callback(null);
				})
			},
			function (callback) {
				if (!dataLoaded) {callback("No Data Loaded"); return;}
				if (blaetterDefined) {callback(null);return;}
				exports.sortAndReduce(DE_AGS.list);
				exports.sortAndReduce(DE_PLZ.list);
				exports.sortAndReduce(AT_AGS.list);
				exports.sortAndReduce(DE_RGS.list);

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
				cb(null);
			}
		)
	}
}
