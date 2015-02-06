var loadDataFromDB = require('./LoadDataFromDB')



var WAApotheke = {
  title : "Wochenaufgabe Apotheke",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["de:amtlicher_gemeindeschluessel"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;'
  },
  keyMap : loadDataFromDB.schluesselMapAGS,
  key: "de:amtlicher_gemeindeschluessel",
  ranktype:"UP"
}

var WAApotheke_AT = {
  title : "Wochenaufgabe Apotheke (AT)",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["ref:at:gkz"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;'
  },
  keyMap : loadDataFromDB.schluesselMapAGS_AT,
  key: "ref:at:gkz",
  ranktype:"UP"
}

var WAApotheke_CH = {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "Wochenaufgabe Apotheke (CH)",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["XXXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["XXX"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;'
  },
  keyMap : loadDataFromDB.schluesselMapAGS_CH,
  key: "XXX",
  ranktype:"UP"
}

var WAApothekePLZ_DE= {
	// Der Key wäre noch zu definieren und die Key Map zu generieren
  title : "Wochenaufgabe Apotheke (DE PLZ)",
  overpass : {
    csvFieldList: '[out:csv(::id,::type,::lat,::lon,::version,::timestamp,::user,name,fixme,phone,"contact:phone",wheelchair;true;";")]',
    query:'[out:json][date:":timestamp:"];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy];\nway(area.a)[amenity=pharmacy];\nrel(area.a)[amenity=pharmacy]);\nout center;',
  	querySub: '[out:json];area["postal_code"=":schluessel:"]->.a;\n(node(area.a)[amenity=pharmacy][:key:];\nway(area.a)[amenity=pharmacy][:key:];\nrel(area.a)[amenity=pharmacy][:key:]);\nout;'
  },
  keyMap : loadDataFromDB.schluesselMapPLZ_DE,
  key: "postal_code",
  ranktype:"UP"
}


var WAAddrWOStreet = {
  title : "Wochenaufgabe Adressen Ohne Strasse",
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
  keyMap : loadDataFromDB.schluesselMapRegio,
  key: "de:regionalschluessel",
  ranktype: "down"
}

var wochenaufgaben= [];
wochenaufgaben["Apotheke"]=WAApotheke;
wochenaufgaben["Apotheke_AT"]=WAApotheke_AT;
wochenaufgaben["AddrWOStreet"]=WAAddrWOStreet;
wochenaufgaben["ApothekePLZ_DE"]=WAApothekePLZ_DE;

exports.map = wochenaufgaben;


