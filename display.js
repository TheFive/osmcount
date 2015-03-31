var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');


var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;
var util     = require('./util.js');
var htmlPage     = require('./htmlPage.js');

var wochenaufgabe = require('./wochenaufgabe.js');

var DataCollection = require('./model/DataCollection.js');
var DataTarget     = require('./model/DataTarget.js');
var WorkerQueue    = require('./model/WorkerQueue.js');





exports.count = function(req,res){
	debug("exports.count");
    var container;
    var collectionName = req.param("collection");
    switch(collectionName) {
        case "DataCollection":container = DataCollection;break;
        case "DataTarget":    container = DataTarget;break;
        case "WorkerQueue":   container = WorkerQueue;break;
        default: container = DataCollection;collectionName = "DataCollection";
    }

    // Fetch the collection test
    container.count({},function handleCollectionCount(err, count) {
    	debug("handleCollectionCount");
    	res.set('Content-Type', 'text/html');
    	if(err) {
    		res.end(JSON.stringify(err));
    	} else {
    		res.end("There are " + count + " records in Collection "+ collectionName);
    	}
    });
}


exports.main = function(req,res){
	var page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname, "html","index.html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));
	page.footer = "OSM Count...";
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	var page = htmlPage.create();

	page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "html",aufgabe+".html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


function listValuesTable(keyname,key,object) {
	if (key == '_id') return "";
	if (object instanceof Date) {
		return "<tr><td>"+keyname+"</td><td>"+object.toString()+"</td></tr>";
	}
	if (object instanceof ObjectID) {
		return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";
	}
	if (typeof(object) == 'object') {
		var result = "";
		for (k in object) {
			if (key) {
			  result += listValuesTable(key+"."+k,k,object[k]);
			} else {
			  result += listValuesTable(k,k,object[k]);
			}

		}
		return result;
    }
    return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";

}

exports.object = function(req,res) {
	debug("exports.object");
	var db = res.db;

	var collectionName = req.params["collection"];
	var objid = req.params["id"];
	//console.log(objid);
	var object=ObjectID(objid);

	db.collection(collectionName).findOne ({_id:object},function handleFindOneObject(err, obj) {
		debug("handleFindOneObject");
		if (err) {
			var text = "Display Object "+ objid + " in Collection "+collectionName;
			text += "Error: "+JSON.stringify(err);
			res.set('Content-Type', 'text/html');
			res.end(text);
		} else {
			text = "<tr><th>Key</th><th>Value</th><tr>";
			text+= listValuesTable("",null,obj);

			page =new htmlPage.create("table");
			page.title = "Data Inspector";
			page.menu ="";
			page.content = '<h1>'+collectionName+'</h1><p><table>'+text+'</table></p>';
			res.set('Content-Type', 'text/html');

			if (collectionName == "WorkerQueue") {
				db.collection("DataCollection").findOne ({schluessel:obj.schluessel,
				                                          source: obj.source},
				                                        function handleFindOneObject(err, obj2) {
					debug("handleFindOneObject2");
					if (err) {
						console.log(JSON.stringify(err));
						res.end(page.generatePage());
					} else {

						var text = "<tr><th>Key</th><th>Value</th><tr>";
						text+= listValuesTable("",null,obj2);


						page.content += '<h1>Zugehörige Daten</h1><p><table>'+text+'</table></p>';
						res.end(page.generatePage());
					}



			})} else res.end(page.generatePage());
		}
	})}








	exports.overpass = function(req,res) {
		debug("exports.overpass");
		var db = res.db;

		var measure = req.params["measure"];
		var schluessel = req.params["schluessel"];


		var sub = req.query.sub;
		if (typeof(sub) == 'undefined') sub = "";
	    var query = generateQuery(measure,schluessel,sub);

		if (!query) query = "Für die Aufgabe "+measure+" ist keine Query definiert";

		var text = "<h1>Overpass Abfrage</h1>"
		text += "<table><tr><td>Aufgabe</td><td>"+measure+"</td></tr> \
							<tr><td>Schl&uuml;ssel</td><td>"+schluessel+"</td></tr></table>";

		text += "<pre>"+query+"</pre>"

		text += '<p>Bitte die Query in die Zwischenablage kopieren und in <a href=http://overpass-turbo.eu>Overpass Turbo</a> einf&uuml;gen</p>';

		text += '<p> Oder <a href=http://overpass-turbo.eu/?Q='+encodeURIComponent(query)+'&R>hier</a> klicken';
		if (measure=="AddrWOStreet") {
			text += '<p>Achtung, die Overpass Abfrage und die Abfrage von User:Gehrke unterschieden sich etwas. Siehe <a href="http://wiki.openstreetmap.org/wiki/DE:Overpass_API/Beispielsammlung#Hausnummern_ohne_Stra.C3.9Fe_finden">wiki</a>.</p>';
		}
		var page = htmlPage.create();
		page.content = text;

		res.set('Content-Type', 'text/html');
	    res.end(page.generatePage());
	}

exports.importApotheken = function(req,res) {
	debug("importApotheken");
    var db = res.db;
    var measure = req.params.measure;
    importCSV.importApothekenVorgabe(measure,db,function ready(err) {
 		var text;
		var text = "Importiert Apotheken "+measure+"<br>";
    	if (err) {
    		text += "Fehler: "+JSON.stringify(err);
    	} else {
			text += " File imported";
		}
		res.set('Content-Type', 'text/html');
		res.end(text);
    });
}












function getValue(columns,object,d) {
  var result;
  //console.log("columns:"+JSON.stringify(columns));
  //console.log("object:"+JSON.stringify(object));
  //console.log("d:"+d);
  if (typeof(object)=='string') return object;
  if (typeof(object)=='undefined') return "";

  if (typeof(columns) == 'object') {

    if (typeof(columns[d])=='string') {
      return getValue(columns,object[columns[d]],d+1);
    }
    else {
    	var result = "";
    	for (var i =d;i<columns.length;i++) {

    		var result2 = getValue(columns[i],object,0);
    		if (result != "" && result2 != "") {
    		  result = result + "," + result2;
    		} else result += result2;
    	}
    	return result;
    }

   	if (typeof(object[columns[d]])=='object') {

   	} else {

   		result = object[columns[d]];
   	}
   } else {
   	result = object[columns];
   }
   return (typeof(result)=='undefined')?"":result;
}


exports.query=function(req,res) {
	debug("exports.query");
	var db = res.db;

    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection;
    var query;
    var options={};
    var queryMenu = "";
    var queryDefined = false;
    switch (req.params.query) {
    	case "DataTarget": collection = db.collection('DataTarget');
    						collectionName = "DataTarget";
    	               columns = ["_id",
    	                          "measure",
    	                          "schluessel",
    	                          "name",
    	                          "apothekenVorgabe",
    	                          "source",
    	                          "linkSource"];
    	               query = {};
    	               if (typeof(req.query.measure) != 'undefined'){
    	               	query.measure = req.query.measure;
    	               }
    	               if (typeof(req.query.schluessel) != 'undefined'){
    	               	query.schluessel = {$regex: "^"+req.query.schluessel};
    	               }
    	               options={"sort":"schluessel"}
    	               break;
    	case "WorkerQueue": collection = db.collection('WorkerQueue');
    						collectionName = "WorkerQueue";
    	               columns = ["_id",
    	                          "type",
    	                          "status",
    	                          "measure",
    	                          "schluessel",
    	                          "prio",
    	                          "exectime",
    	                          "timestamp",
    	                          "error",
    	                          ["Error Code","error","code"],
    	                          ["Error Status Code","error","statusCode"]];
    	               query = {};
    	               if (typeof(req.query.type) != 'undefined'){
    	               	query.type = req.query.type;
    	               }
    	               if (typeof(req.query.status) != 'undefined'){
    	               	query.status = req.query.status;
    	               }
    	               if (typeof(req.query.measure) != 'undefined'){
    	               	query.measure = req.query.measure;
    	               }

     	               options={"sort":"exectime"}
    	               break;
    	case "pharmacy": collection = db.collection('POI');
    	                 collectionName = "POI";
    	                  columns = ["Links",
    	                             ["name","tags","name"],
    	                             ["state","nominatim","state"],
    	                             ["state_destrict","nominatim","state_district"],
    	                             ["county","nominatim","county"],
    	                             ["PLZ","nominatim","postcode"],
    	                             ["Town",["nominatim","town"],["nominatim","village"],["nominatim","city"]],
    	                             ["Straße",["nominatim","road"],["nominatim","pedestrian"]],
    	                             ["Hausnummer","nominatim","house_number"],
    	                             ["Öffnungszeiten","tags","opening_hours"],
    	                             ["Operator","tags","operator"],
    	                             ["Telefon",["tags","phone"],["tags","contact:phone"]],
    	                             ["Fax",["tags","fax"],["tags","contact:fax"]],
    	                             ["website",["tags","website"],["tags","contact:website"]],
    	                             ["wheelchair","tags","wheelchair"],

    	                             ];
    	                  query = {};options={};
    	                  queryMenu = "";

    	                  if (typeof(req.query.state)!='undefined' && req.query.state != "") {
    	                    queryDefined = true;
    	                    query["nominatim.state"] =req.query.state;
    	                    queryMenu += 'state:<input type="text" name="state" value="'+req.query.state+'">';
    	                  } else queryMenu += 'state:<input type="text" name="state">';

   	                      if (typeof(req.query.state_district)!='undefined' && req.query.state_district != "") {
    	                    queryDefined = true;
    	                    query["nominatim.state_district"] =req.query.state_district;
    	                    queryMenu += 'state_district:<input type="text" name="state_district" value="'+req.query.state_district+'">';
    	                  } else queryMenu += 'state_district:<input type="text" name="state_district">';

   	                      if (typeof(req.query.county)!='undefined' && req.query.county != "") {
    	                    queryDefined = true;
    	                    query["nominatim.county"] =req.query.county;
    	                    queryMenu += 'county:<input type="text" name="county" value="'+req.query.county+'">';
    	                  } else queryMenu += 'county:<input type="text" name="county">';

    	                  if (typeof(req.query.postcode)!='undefined' && req.query.postcode != "") {
    	                    queryDefined = true;
    	                    query["nominatim.postcode"] =req.query.postcode;
    	                    queryMenu += 'postcode:<input type="text" name="postcode" value="'+req.query.postcode+'">';
    	                  } else queryMenu += 'postcode:<input type="text" name="postcode">';


     	                  if (typeof(req.query.city)!='undefined' && req.query.city != "") {
    	                    queryDefined = true;
    	                    var q = [{"nominatim.town":req.query.city},
    	                             {"nominatim.city":req.query.city},
    	                             {"nominatim.village":req.query.city}];
    	                    query["$or"] =q;
    	                    queryMenu += 'town:<input type="text" name="city" value="'+req.query.city+'">';
    	                  } else queryMenu += 'town:<input type="text" name="city">';

    	                  if (typeof(req.query.country)!='undefined' && req.query.country != "") {
    	                    queryDefined = true;
    	                    query["nominatim.country"] =req.query.country;
    	                    queryMenu += 'country:<input type="text" name="country" value="'+req.query.country+'">';
    	                  } else queryMenu += 'country:<input type="text" name="country">';

    	                  if (typeof(req.query.missing)!='undefined' && req.query.missing != "") {
    	                    queryDefined = true;
    	                    query["nominatim.timestamp"] ={$exists:0};
    	                    query["overpass.loadBy"] = req.query.missing;
     	                  }
     	                  queryMenu += 'missing nominatim: <select name="missing"> \
    	                                   <option value="">none</option> \
    	                                  <option value="DE">DE</option> \
    	                                  <option value="AT">AT</option> \
    	                                  <option value="CH">CH</option> \
    	                                  </select>';





    	                  queryMenu = '<form>'+queryMenu+'<input type="submit"></form>';
    	                  break;
    	 default:collection = db.collection('WorkerQueue');
    	               columns = ["_id","type","status","measure"];
    	               query = {type:"insert"};
    }
    if (collectionName != "POI" || queryDefined) {
    collection.find(query,options).toArray(function(err,data) {
    	if (err) {
    		res.set('Content-Type', 'text/html');
			res.end(JSON.stringify(err)+JSON.stringify(query));

 	   		console.log("Error: "+err);
 	   		return;
    	}
    	table = "";
    	tablerow = "";
    	for (j=0;j<columns.length;j++) {
    		header = columns[j];
    		if (typeof(columns[j])=='object') {
    			header = columns[j][0];
    		}
    		tablerow += "<th>"+header+"</th>";
    	}
    	tablerow = "<tr>"+tablerow+"</tr>";
    	table += tablerow;
    	for (i = 0;i<data.length;i++) {
    	  tablerow = "";
    	  d = data [i];
    	  tablerow = '<td> <a href="/object/'+collectionName+'/'+d._id+'.html">'+d._id+'</a></td>';
    	  if (req.params.query == "pharmacy") {
    	    link1 = '<a href="/object/'+collectionName+'/'+d._id+'.html">Data</a>';
    	    link2 = '<a href="https://www.openstreetmap.org/'+d.type+'/'+d.id+'">OSM</a>';
    	     tablerow = '<td>'+link1+"  "+ link2+'</td>';
    	  }
    	  key = columns[j];
    	  for (j=1;j<columns.length;j++) {
      		tablerow += "<td>"+getValue(columns[j],d,1);+"</td>";
    	  }
    	  tablerow = "<tr>"+tablerow+"</tr>";
    	  table += tablerow;
    	}
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.menu =queryMenu;
		page.content = '<p><table>'+table+'</table></p>';
		page.footer = "Ergebnisse: " + data.length + " Abfrage: "+JSON.stringify(query)+"<br>Aktuelle Zeit: "+new Date();
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;

    })
    } else {
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.menu =queryMenu;
		page.content = '<p>Bitte die POIs durch eine Query Einschränken</p>';
		page.footer = JSON.stringify(query);
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;

    }


}
