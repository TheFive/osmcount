


queryApotheke='area["de:regionalschluessel"="03157"];(node(area)[amenity=pharmacy];way(area)[amenity=pharmacy];rel(area)[amenity=pharmacy]);out ids;'



queryAddrWOStreet='area[type=boundary]["de:amtlicher_gemeindeschluessel"="03157"]->.boundaryarea; \
rel(area.boundaryarea)[type=associatedStreet]->.associatedStreet; \
 \
way(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesWay; \
way(r.associatedStreet:"house")->.asHouseWay; \
((.allHousesWay; - .asHouseWay); >; );out ids; \
 \
node(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesNode; \
node(r.associatedStreet:"house")->.asHouseNode; \
((.allHousesNode; - .asHouseNode););out ids; \
 \
rel(area.boundaryarea)["addr:housenumber"]["addr:street"!~"."]["addr:place"!~"."]->.allHousesRel; \
rel(r.associatedStreet:"house")->.asHouseRel; \
((.allHousesRel; - .asHouseRel); >>; );out ids;' 

queryBoundaries='[out:json][timeout:900];area[type=boundary]["int_name"="Deutschland"]["admin_level"="2"];rel(area)[admin_level];out; \
' 



var request = require('request');

function overpassQuery(query, cb, options) {
    options = options || {};
    request.post(options.overpassUrl || 'http://overpass-api.de/api/interpreter', function (error, response, body) {
        var geojson;

        if (!error && response.statusCode === 200) {
            cb(undefined, body);
        } else if (error) {
            cb(error);
        } else if (response) {
            cb({
                message: 'Request failed: HTTP ' + response.statusCode,
                statusCode: response.statusCode
            });
        } else {
            cb({
                message: 'Unknown error.',
            });
        }
    }).form({
        data: query
    });
};


// Dubliziere die Verbindung mit der mongdb

var fs, configurationFile;

configurationFile = 'configuration.json';
fs = require('fs');

var configuration = JSON.parse(
  fs.readFileSync(configurationFile)
);

// Log the information from the file
console.log(configuration);


boundariesFile = 'Boundaries OSM Nov 14.json';
fs = require('fs');

var boundariesJSON = JSON.parse(
  fs.readFileSync(boundariesFile)
);

var mongodb;

var mongodbConnectStr ='mongodb://'
                   + configuration.username + ':'
                   + configuration.password + '@'
                   + configuration.database;
                   
console.log("connect:"+mongodbConnectStr);
var MongoClient = require('mongodb').MongoClient;

MongoClient.connect(mongodbConnectStr, function(err, db) {
  if (err) throw err;
  mongodb = db;
  console.log("Connected to Database mosmcount");
  mongodb.createCollection("OSMBoundaries");
 for (i = 0; i<boundariesJSON.elements.length;i++)
    {
      mongodb.collection("OSMBoundaries").insert(boundariesJSON.elements[i].tags, function(err,doc){});
      
    }

  })











/*
overpassQuery(queryBoundaries,function(error, data) {
  if (error) {
  	console.log("Fehler bei Overpass Abfrage");
    console.dir(error);
  } else {
    var result = new Object;
    result.count=0;
    result.data=data;
    length= data.length;
    for (i=0;i<length;i++) {
    	if (data.charAt(i) != 'i')  {continue}
    	if (data.charAt(i+1) != 'd')  {continue}
    	if (data.charAt(i+2) != '=')  {continue}
    	if (data.charAt(i+3) != '"')  {continue}
    	result.count++;
    }
    //console.dir(result);
    r = JSON.parse(result);
    for (i = 0; i<r.elements.length;i++)
    {
      console.log(r.elements[i].tags.name);
      
    }
  }
})
*/

   