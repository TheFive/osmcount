


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

console.log("Starting Query");
overpassQuery(queryApotheke,function(error, data) {
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
    
    console.dir(result);
    return result;
    
    
  
  }
  
})
