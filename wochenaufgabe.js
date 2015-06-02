var debug = require('debug')('wochenaufgabe');
var util = require('./util.js');
var loadDataFromDB = require('./model/LoadDataFromDB.js');


function tagCounterInit(result) {
  debug('tagCounterInit');
  result.missing = {};
  result.existing = {}
  result.existing.fixme = 0;
  result.missing.opening_hours = 0;
  result.missing.phone=0;
  result.missing.wheelchair = 0;
  result.missing.name = 0;
}

function tagCounterCount(p,result) {
  debug('tagCounterCount');
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

exports.tagCounter = function tagCounter(osmdata,result) {
  debug('tagCounter');
  tagCounterInit(result);
  for (var i = 0 ; i< osmdata.length;i++ ) {
	  tagCounterCount(osmdata[i].tags,result);
  }
}


exports.tagCounter2 = function tagCounter2(osmdata,keyList,key,defJson) {
  debug('tagCounter2');
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
  for (var i =0;i<keyList.length;i++) {
    var m = util.clone(defJson);
    m.schluessel = keyList[i];
    m.count = 0;
    map[keyList[i]]=m;
    tagCounterInit(m);
  }
  for (var i=0; i< osmdata.length;i++) {
    for (var z=0;z<osmdata[i].osmArea.length;z++) {
      var schluessel = osmdata[i].osmArea[z][key];
      var m = map[schluessel];
      if (typeof(m) != 'undefined') {
        tagCounterCount(osmdata[i].tags,m);
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
  title : "Wochenaufgabe Apotheke",
  name: "Apotheke",
  description: "Apotheke",
  osmArea: "Deutschland",
  country_code: "DE",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;',
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
  tagCounter : exports.tagCounter,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:7
}

var WAApotheke_AT = {
  title : "Wochenaufgabe Apotheke (AT)",
  name : "Apotheke_AT",
  description: "Apotheke",
  osmArea: "Österreich",
  country_code: "AT",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;',
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
  tagCounter:exports.tagCounter,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:14
}

var WAApotheke_CH = {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "Wochenaufgabe Apotheke (CH)",
  name : "Apotheke_CH",
  description: "Apotheke",
  osmArea:"Schweiz",
  country_code: "CH",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["XXXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["XXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;',
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
  tagCounter:exports.tagCounter,
  tagCounterGlobal:exports.tagCounter2,
  overpassEveryDays:14
}

var WAApothekePLZ_DE= {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "Wochenaufgabe Apotheke (DE PLZ)",
  name: "Apotheke LZ_DE",
  description: "Apotheke",
  osmArea: "Deutschland",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;'
  },

  map: loadDataFromDB.DE_PLZ,
  key: "postal_code",
  ranktype:"UP",
  tagCounter:null,
  tagCounterGlobal:null,
  overpassEveryDays:28
}


var WAAddrWOStreet = {
  title : "Wochenaufgabe Adressen Ohne Strasse",
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
  tagCounterGlobal: null
}

var wochenaufgaben= [];
wochenaufgaben["Apotheke"]=WAApotheke;
wochenaufgaben["Apotheke_AT"]=WAApotheke_AT;
wochenaufgaben["AddrWOStreet"]=WAAddrWOStreet;
wochenaufgaben["ApothekePLZ_DE"]=WAApothekePLZ_DE;

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
