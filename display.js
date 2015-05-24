var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');


var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var util     = require('./util.js');
var htmlPage     = require('./htmlPage.js');

var wochenaufgabe = require('./wochenaufgabe.js');

var DataCollection = require('./model/DataCollection.js');
var DataTarget     = require('./model/DataTarget.js');
var WorkerQueue    = require('./model/WorkerQueue.js');
var POI    = require('./model/POI.js');

















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
  if (object == null) return "";

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
    	case "DataTarget": collection = DataTarget;
    						collectionName = "DataTarget";
    	               columns = ["id",
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
    	               	query.schluessel = req.query.schluessel;
    	               }
    	               options={"sort":"schluessel"}
    	               break;
    	case "WorkerQueue": collection = WorkerQueue;
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
                       if (typeof(req.query.schluessel) != 'undefined'){
                        query.schluessel = req.query.schluessel;
                       }

     	               options={"sort":"exectime"}
    	               break;
    	case "pharmacy": collection = POI;
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
            query = "";
            queryMenu = "";

            function genQueryAndMenu(p) {
              if (typeof(req.query[p])!='undefined' && req.query[p] != "") {
                if (query != "") query += " and ";
                query += "(data->'nominatim'->>'"+p+"'='"+req.query[p]+"')";
                queryMenu += '<label for="'+p+'">'+p+':</label><input class="form-control" type="text" name="'+p+'" value="'+req.query[p]+'">';
              } else queryMenu += '<label for="'+p+'">'+p+':</label><input class="form-control" type="text" name="'+p+'">';

            }
            genQueryAndMenu("country");
            genQueryAndMenu("country_code");
            genQueryAndMenu("state");
            genQueryAndMenu("postcode");
            genQueryAndMenu("state_district");
            genQueryAndMenu("county");

              if (typeof(req.query.city)!='undefined' && req.query.city != "") {
              if (query != "") query += " and ";
              query += " ((data->'nominatim'->>'town'='"+req.query.city+"') or " +
                       "  (data->'nominatim'->>'city'='"+req.query.city+"') or " +
                       "  (data->'nominatim'->>'village'='"+req.query.city+"'))";
              queryMenu += '<label for="town">town:</label><input class="form-control" type="text" name="city" value="'+req.query.city+'">';
            } else queryMenu += '<label for="town">town:</label><input class="form-control" type="text" name="city">';

     
            if (typeof(req.query.missing)!='undefined' && req.query.missing != "") {
              if (query != "") query += " and ";
              query += "((data->'nominatim'->'timestamp') is null)";
              query += "and (data->'overpass'->>'loadBy'='"+req.query.missing+"')";
              }
            if (query != "") query = "where "+query;
              queryMenu += '<label for="missing nominatim">missing nominatim</label> <select class="form-control" name="missing"> \
                             <option value="">none</option> \
                            <option value="DE">DE</option> \
                            <option value="AT">AT</option> \
                            <option value="CH">CH</option> \
                            </select>';


            break;
    	 default:collection = WorkerQueue;
    	               columns = ["_id","type","status","measure"];
    	               query = {type:"insert"};
    }
    if (collectionName != "POI" || query != "") {
    collection.find(query,options,function(err,data) {
    	if (err) {
    		res.set('Content-Type', 'text/html');
			res.end("Fehler beim Find: \n"+JSON.stringify(err)+JSON.stringify(query));

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
    	  tablerow = '<td> <a href="/object/'+collectionName+'/'+d.id+'.html">'+d.id+'</a></td>';
    	  if (req.params.query == "pharmacy") {
    	    link1 = '<a href="/object/'+collectionName+'/'+d.id+'.html">Data</a>';
    	    link2 = '<a href="https://www.openstreetmap.org/'+d.type+'/'+d.id+'">OSM</a>';
    	     tablerow = '<td>'+link1+"  "+ link2+'</td>';
    	  }
    	  var key = columns[j];
    	  for (j=1;j<columns.length;j++) {
      		tablerow += "<td>"+getValue(columns[j],d,1);+"</td>";
    	  }
    	  tablerow = "<tr>"+tablerow+"</tr>";
    	  table += tablerow;
    	}
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;

    if (req.params.query == "pharmacy") {
      page.title = "Apothekenliste"
    }
		page.modal =queryMenu;
		page.content = '<p><table class="table-condensed table-bordered table-hover">'+table+'</table></p>';
		page.footer = "Ergebnisse: " + data.length + " Abfrage: "+JSON.stringify(query)+"<br>Aktuelle Zeit: "+new Date();
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;

    })
    } else {
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.modal =queryMenu;
		page.content = '<p>Bitte die POIs durch eine Query Einschränken</p>';
		page.footer = JSON.stringify(query);
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;

    }


}


 