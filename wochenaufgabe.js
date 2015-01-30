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
wochenaufgaben["AddrWOStreet"]=WAAddrWOStreet;

exports.map = wochenaufgaben;


