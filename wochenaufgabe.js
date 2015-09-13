var should = require('should');
var wochenaufgabe_data = require('./wochenaufgabe_data.js');
var debug = require('debug')('wochenaufgabe');
var util = require('./util.js');
var loadDataFromDB = require('./model/LoadDataFromDB.js');




//Wochenaufgaben will be described with the WA Objects
/* a WA Object has the following properties
nitle: The Title that is displayed in the differend Views
name: The Name, that is referenced in all Data 
description: Name of the HTML File, describing the Wochenaufgabe 
osmArea: Experimental
country_code: Experimental
ssl:List, describing the subtags that will be counted by the Wochenaufgabe,
overpass. Several Queries for Overpass counting the values
map: Map generated by LoadDataFrom DB, describing
     maps to name the Keys and list to run overpass for
map.keyList: Alternativ to map formn LoadDataFrom DB
     The more flexible solution, as this can handle several key types in one Wochenaufgabe
     But the WA Internal Key has to fullfill the OSMCount "part" Rule   
key: key that is used for the Query (if keyList is not used)
ranktype: how to sort in the table,
tagCounter : Function to count subtacs, should be in line with ssl
tagCounterGlobal:experimental
  overpassEveryDays: how often should the Wochenaufgabe be runned
}
*/

// this Function initialises the Result with 0 for each member
// of the given SubSelectorList (ssl)
function tagCounterInit(result,ssl) {
  debug('tagCounterInit');
  should.exist(result);
  should.exist(ssl);
  result.missing = {};
  result.existing = {}
  for (var k in ssl) {
    var o = ssl[k];
    result[o.type][o.prop]=0;
  }
}


// This is the subSelectorList for nothing to count
var noSubSelectorList = {};


// This is the Subselector List for Pharmacies
var apothekenSubSelectorList = {
  "Name":{type:"missing",prop:"name"},
  "Öffnungszeiten":{type:"missing",prop:"opening_hours"},
  "fixme":{type:"existing",prop:"fixme"},
  "phone":{type:"missing",prop:"phone",osmprops:["phone","contact:phone"]},
  "Wheelchair":{type:"missing",prop:"wheelchair"}
}

//This is the SubSelector List for the GuidePost Nodes
var guidePostSubSelectorList = {
  "hiking":{type:"existing",prop:"hiking"},
  "bicycle":{type:"existing",prop:"bicycle"},
  "note:destination":{type:"existing",prop:"note:destination"},
  "name":{type:"existing",prop:"name"},
  "inscription":{type:"existing",prop:"inscription"},
  "description":{type:"existing",prop:"description"},
  "operator":{type:"existing",prop:"operator"},
  "ref":{type:"existing",prop:"ref"},
  "material":{type:"existing",prop:"material"}
}

//This is the SubSelector List for the GuidePost relations
var destinationRelSubSelectorList = {
  "destination":{type:"existing",prop:"destination"},
  "time":{type:"existing",prop:"time"},
  "distance":{type:"existing",prop:"distance"},
}

var destinationPathSubSelectorList = {
  "destination:f/b":{type:'existing',prop:"destination:f/b",osmprops:['destination:forward','destination:backward']},
  "destination:symbol:f/b":{type:'existing',prop:"destination:symbol:f/b",osmprops:['destination:symbol:forward','destination:symbol:backward']},
  "destination:ref:f/b":{type:'existing',prop:"destination:ref:f/b",osmprops:['destination:ref:forward','destination:ref:backward']},
  "destination:lang:f/b":{type:'existing',prop:"destination:lang:f/b",osmprops:['destination:lang:forward','destination:lang:backward']},
  "destination:NESW":{type:'existing',prop:"destination:NESW",osmprops:['destination:N','destination:NE','destination:E','destination:SE','destination:S','destination:SW','destination:W','destination:NW']}
}

//This is the SubSelector List for the GuidePost Nodes
var iceCreamSubSelectorList = {
  "Name":{type:"missing",prop:"name"},
  "Outdoor Seating":{type:"missing",prop:"outdoor_seating"},
  "Opening Hours":{type:"missing",prop:"opening_hours"},
  "fixme":{type:"existing",prop:"fixme"},
  "Website":{type:"missing",prop:"website"},
  "Phone":{type:"missing",prop:"phone",osmprops:["phone","contact:phone"]},
  "Wheelchair":{type:"missing",prop:"wheelchair"}
}

// This function looks up every tag in the subselector list
// and increases the corresponding result counter 
// if necessary
function tagCounterGeneric(p,result,ssl) {
  debug('tagCounterGeneric');

  for (var k in ssl) {
    var o = ssl[k];
    if (o.type == "existing") {
      if (typeof(o.osmprops)== 'undefined') {
        if (p.hasOwnProperty(o.prop)) {
          result.existing[o.prop] +=1;
        }
      }
      else {
        for (var i=0;i<o.osmprops.length;i++) {
          if (p.hasOwnProperty(o.osmprops[i])) {
            result.existing[o.prop] +=1;
            break;
          }
        }
      }
    }
    if (o.type == "missing") {
      if (typeof(o.osmprops)== 'undefined') {
        if (!p.hasOwnProperty(o.prop)) {
          result.missing[o.prop] +=1;
        }
      }
      else {
        var mis = 1;
        for (var i=0;i<o.osmprops.length;i++) {
          if (p.hasOwnProperty(o.osmprops[i])) {
            mis = 0;
            break;
          }
        }
        result.missing[o.prop] += mis;
      }
    }
  }
}



exports.tagCounterPharmacy = function tagCounterPharmacy(osmdata,result) {
  debug('tagCounterPharmacy');
  tagCounterInit(result,apothekenSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
	  tagCounterGeneric(osmdata[i].tags,result,apothekenSubSelectorList);
  }
}
exports.tagCounterNoSubSelector = function tagCounter(osmdata,result) {
  debug('tagCounterNoSubSelector');
  tagCounterInit(result,noSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
    tagCounterGeneric(osmdata[i].tags,result,noSubSelectorList);
  }
}


exports.tagCounterGuidePost = function tagCounter(osmdata,result) {
  debug('tagCounterGuidePost');
  tagCounterInit(result,guidePostSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
    tagCounterGeneric(osmdata[i].tags,result,guidePostSubSelectorList);
  }  
}

exports.tagCounterDestinationRel = function tagCounter(osmdata,result) {
  debug('tagCounterDestinationRel');
  tagCounterInit(result,destinationRelSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
    tagCounterGeneric(osmdata[i].tags,result,destinationRelSubSelectorList);
  }  
}

exports.tagCounterDestinationPath = function tagCounter(osmdata,result) {
  debug('tagCounterDestinationRel');
  tagCounterInit(result,destinationPathSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
    tagCounterGeneric(osmdata[i].tags,result,destinationPathSubSelectorList);
  }  
}

exports.tagCounterIceCream = function tagCounter(osmdata,result) {
  debug('tagCounterIceCream');
  tagCounterInit(result,iceCreamSubSelectorList);
  for (var i = 0 ; i< osmdata.length;i++ ) {
    tagCounterGeneric(osmdata[i].tags,result,iceCreamSubSelectorList);
  }  
}




// Experimental Function 
// not used yet
function createDC(defJson,keyType,key) {
  var r = util.clone(defJson);
  r.schluessel = key;
  r.keyType = keyType;
  r.count = 0;
  tagCounterInit(r);
  return r;
}

// Experimental Function 
// not used yet

exports.tagCounter2 = function tagCounter2(osmdata,keyList,key,defJson) {
  debug('tagCounter2');
  console.log("Hallo");
  console.log(defJson);
  (typeof(defJson)).should.equal('object');
  // osmdata
  //   osmElemente, that are annotated by osmArea Json Array, that contains tag key
  // keyList
  //   List of keys, that has to be count
  // Key
  //   tag for the list of keys
  // defJson
  //   default Measure Object, that is copied for every element in keyList
  var map = {};




  debug("Checking "+keyList.length+" keys for key "+key);

  for (var i=0; i< osmdata.length;i++) {
    var od = osmdata[i];
    for (var z=0;z<od.osmArea.length;z++) {
      var oa = od.osmArea[z];
      for (var k in oa) {
        var schluessel = osmdata[i].osmArea[z][k];
        var m = map[k+m];
        if (typeof(m) == 'undefined') {
          m = createDC(defJson,oa.keyType,key).
          map[k+m]= m;
        }
        tagCounterCount(od.tags,m);
        m.count ++;
      }
    }
  }
  var result = [];
  for (var k in map) {
    result.push(map[k]);
  }
  return result;
}



    


var WAApotheke = {
  title : "WA Apotheke",
  name: "Apotheke",
  description: "Apotheke",
  osmArea: "Deutschland",
  country_code: "DE",
  ssl:apothekenSubSelectorList,
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][timeout:900][date:":timestamp:"];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:subkey:];\nway(area.a)[amenity=pharmacy][:subkey:];\nrel(area.a)[amenity=pharmacy][:subkey:]);\nout;',
    fullQuery: '[out:json][timeout:5000];area[name="Deutschland"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["de:amtlicher_gemeindeschluessel"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["de:amtlicher_gemeindeschluessel"]; \
          out;'
  },
  map : loadDataFromDB.DE_AGS,
  key: "de:amtlicher_gemeindeschluessel",
  ranktype:"UP",
  tagCounter : exports.tagCounterPharmacy,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:28,
  defaultLengthOfTime:7,
  wochenaufgabe_start:'2015-01-31'
}

var WAApotheke_AT = {
  title : "WA Apotheke (AT)",
  name : "Apotheke_AT",
  description: "Apotheke",
  osmArea: "Österreich",
  country_code: "AT",
  ssl:apothekenSubSelectorList,
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][timeout:900][date:":timestamp:"];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:subkey:];\nway(area.a)[amenity=pharmacy][:subkey:];\nrel(area.a)[amenity=pharmacy][:subkey:]);\nout;',
    fullQuery: '[out:json][timeout:4000];area[name="Österreich"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:at:gkz"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["ref:at:gkz"]; \
          out;'
  },
  map: loadDataFromDB.AT_AGS,
  key: "ref:at:gkz",
  ranktype:"UP",
  tagCounter:exports.tagCounterPharmacy,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:28,
  defaultLengthOfTime:7,
  wochenaufgabe_start:'2015-01-31'
}

var WAApotheke_CH = {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "WA Apotheke (CH)",
  name : "Apotheke_CH",
  description: "Apotheke",
  osmArea:"Schweiz",
  country_code: "CH",
  ssl:apothekenSubSelectorList,
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][timeout:900][date:":timestamp:"];area["XXXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["XXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:subkey:];\nway(area.a)[amenity=pharmacy][:subkey:];\nrel(area.a)[amenity=pharmacy][:subkey:]);\nout;',
    fullQuery: '[out:json][timeout:3600];area[name="Schweiz"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:bfs_Gemeindenummer"];out ids; ); \
          .pharmacies is_in; \
          area._[boundary=administrative]; \
          out;'
  },

  map: loadDataFromDB.CH_AGS,
  key: "XXX",
  ranktype:"UP",
  tagCounter:exports.tagCounterPharmacy,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:28,
  defaultLengthOfTime:7,
  wochenaufgabe_start:'2015-01-31'
}

var WAApothekePLZ_DE= {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "WA Apotheke (DE PLZ)",
  name: "Apotheke LZ_DE",
  description: "Apotheke",
  osmArea: "Deutschland",
  ssl:apothekenSubSelectorList,
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][timeout:900][date:":timestamp:"];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:subkey:];\nway(area.a)[amenity=pharmacy][:subkey:];\nrel(area.a)[amenity=pharmacy][:subkey:]);\nout;'
  },

  map: loadDataFromDB.DE_PLZ,
  key: "postal_code",
  ranktype:"UP",
  tagCounter:null,
  tagCounterGlobal:null,
  overpassEveryDays:28,
  defaultLengthOfTime:7,
  wochenaufgabe_start:'2015-01-31'
}


var WAAddrWOStreet = {
  title : "WA Adressen Ohne Strasse",
  name: "AddrWOStreet",
  overpass: {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user;true;";")]',
    query: '[out:json][timeout:900];area[type=boundary]["de:regionalschluessel"=":schluessel:"]->.boundaryarea;\n\
rel(area.boundaryarea)[type=associatedStreet]->.associatedStreet; \n\
\n\
way(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesWay; \n\
way(r.associatedStreet:"house")->.asHouseWay; \n\
(.allHousesWay; - .asHouseWay); out ids; \n\
 \n\
node(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesNode; \n\
node(r.associatedStreet:"house")->.asHouseNode; \n\
(.allHousesNode; - .asHouseNode);out ids; \n\
 \n\
rel(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesRel; \n\
rel(r.associatedStreet:"house")->.asHouseRel; \n\
(.allHousesRel; - .asHouseRel);out ids;',
	querySub: this.query
  },

  map: loadDataFromDB.DE_RGS,
  key: "de:regionalschluessel",
  ranktype: "down",
  tagCounter: null,
  tagCounterGlobal: null,
  defaultLengthOfTime:7
}


var WAApothekeTestDE = {
  title : "WA Apotheke Test",
  name: "Apotheke Haan",
  description: "Apotheke",
  osmArea: "Haan",
  country_code: "DE-HAAN",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][timeout:900][date:":timestamp:"];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
    querySub: '[out:json];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:subkey:];\nway(area.a)[amenity=pharmacy][:subkey:];\nrel(area.a)[amenity=pharmacy][:subkey:]);\nout;',
    fullQuery: '[out:json][timeout:5000];area[name="Haan"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["de:amtlicher_gemeindeschluessel"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["de:amtlicher_gemeindeschluessel"]; \
          out;'
  },
  map : loadDataFromDB.DE_AGS,
  key: "de:amtlicher_gemeindeschluessel",
  ranktype:"UP",
  tagCounter : exports.tagCounterPharmacy,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:2,
  defaultLengthOfTime:7
}

var WA_GuidePost_Node = {
  title : "WA Wanderwegweiser (information = guidepost)",
  name: "GuidePost_Node",
  description: "GuidePost",
  osmArea: "Europa",
  country_code: "EU",
  ssl:guidePostSubSelectorList,
  overpass : {
    query:'[out:json][timeout:900][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(node(area.a)[information=guidepost];);\nout center;',
    querySub:'[out:json][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(node(area.a)[information=guidepost][:subkey];);\nout center;',
  },
  map: {
  keyList: wochenaufgabe_data.dachKeyList

  },
  ranktype:"UP",
  tagCounter : exports.tagCounterGuidePost,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:2,
  defaultLengthOfTime:10,
  //overpassUrl:'http://dev.overpass-api.de/api_mmd/interpreter',
  overpassUrl:'http://dev.overpass-api.de/api_mmd_test_only/interpreter',
  wochenaufgabe_start:'2015-07-07'
}
var WA_GuidePost_Rel = {
  title : "WA Wanderwegweiser (type=destination_sign)",
  name: "GuidePost_Rel",
  description: "GuidePost",
  osmArea: "Europa",
  country_code: "EU",
  ssl:destinationRelSubSelectorList,
  overpass : {
    query:'[out:json][timeout:900][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(relation(area.a)[type=destination_sign];);\nout center;',
    querySub:'[out:json][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(relation(area.a)[type=destination_sign][:subkey];);\nout center;',
  },
  map: {
  keyList: wochenaufgabe_data.dachKeyList

  },
  ranktype:"UP",
  tagCounter : exports.tagCounterDestinationRel,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:2,
  defaultLengthOfTime:10,
  //overpassUrl:'http://dev.overpass-api.de/api_mmd/interpreter',
  overpassUrl:'http://dev.overpass-api.de/api_mmd_test_only/interpreter',
  wochenaufgabe_start:'2015-07-07'
}



var WA_GuidePost_Path = {
  title : "WA Wanderwegweiser (path with Destination)",
  name: "GuidePost_Path",
  description: "GuidePost",
  osmArea: "Europa",
  country_code: "EU",
  ssl:noSubSelectorList,
  overpass : {
    query:'[out:json][timeout:9000][date:":timestamp:"];area[":key:"=":value:"]->.a;(way(area.a)[highway=path][~"destination:.*"~".*"];way(area.a)[highway=footway][~"destination:.*"~".*"];way(area.a)[highway=cycleway][~"destination:.*"~".*"];way(area.a)[highway=track][~"destination:.*"~".*"];);\nout tags;',
    querySub:'not defined',
  },
  map: {
  keyList: wochenaufgabe_data.dachKeyList

  },
  ranktype:"UP",
  tagCounter : exports.tagCounterDestinationPath,
  ssl:destinationPathSubSelectorList,

  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:2,
  defaultLengthOfTime:10,
  //overpassUrl:'http://dev.overpass-api.de/api_mmd/interpreter',
  overpassUrl:'http://dev.overpass-api.de/api_mmd_test_only/interpreter',
  wochenaufgabe_start:'2015-07-07'
}


var WA_IceCream = {
  title : "WA Ice Cream",
  name: "IceCream",
  description: "IceCream",
  osmArea: "Europa",
  country_code: "EU",
  ssl:iceCreamSubSelectorList,
  overpass : {
    query:'[out:json][timeout:900][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(node(area.a)[amenity=ice_cream];);\nout center;',
    querySub:'[out:json][date:":timestamp:"];area[":key:"=":value:"]->.a;\n(node(area.a)[amenity=ice_cream][:subkey];);\nout center;',
  },
  map: {
  keyList: wochenaufgabe_data.dachKeyList

  },
  ranktype:"UP",
  tagCounter : exports.tagCounterIceCream,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:2,
  defaultLengthOfTime:10,
  //overpassUrl:'http://dev.overpass-api.de/api_mmd/interpreter',
  overpassUrl:'http://dev.overpass-api.de/api_mmd_test_only/interpreter',
  wochenaufgabe_start:'2015-09-XX'
}

var wochenaufgaben= [];
wochenaufgaben["Apotheke"]=WAApotheke;
wochenaufgaben["Apotheke_AT"]=WAApotheke_AT;
wochenaufgaben["AddrWOStreet"]=WAAddrWOStreet;
wochenaufgaben["ApothekePLZ_DE"]=WAApothekePLZ_DE;
wochenaufgaben["GuidePost_Node"]=WA_GuidePost_Node;
wochenaufgaben["GuidePost_Rel"]=WA_GuidePost_Rel;
wochenaufgaben["GuidePost_Path"]=WA_GuidePost_Path;
wochenaufgaben["IceCream"]=WA_IceCream;

exports.map = wochenaufgaben;



exports.boundaryPrototype = 
{
  "name":1,
  "de:regionalschluessel":1,
  "postal_code":1,
  "ref:at:gkz":1,
  "de:amtlicher_gemeindeschluessel":1,
  boundary:1,
  admin_level:1,
  "de:regionalschluessel":1,
  "osmcount_country":1
}
