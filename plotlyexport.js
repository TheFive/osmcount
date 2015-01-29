var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('plotyexport');
  debug.data = require('debug')('plotyexport:data');
  debug.entry = require('debug')('plotyexport:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var htmlPage = require('./htmlPage.js');

var async    = require('async');



function getKreisname(schluessel,kreisnamen) {
	if (schluessel =="") return "Deuschland";
  	if (typeof(kreisnamen[schluessel])=='undefined') return schluessel;
	n = kreisnamen[schluessel].name;
	n = n.replace("ü","ue");
	n = n.replace("ö","ae");
	n = n.replace("ä","ae");
	n = n.replace("ß","ss");
    return n;
}

exports.plot = function(req,res){
	debug.entry("exports.plot");
	var db = configuration.getDB();
	
	var wochenaufgabe = req.param("measure");
    var location = req.param("location");
    if (typeof(location) =='undefined') location="";
    var lok = req.param("lok");
    var lengthOfKey = 2;
    if (typeof(parseInt(lok))=='number') lengthOfKey = parseInt(lok);
    
    var collection = db.collection('DataCollection');
   

	var kreisnamen = loadDataFromDB.schluesselMapAGS;	 
 
 
    valueToCount = "$count";
   	valueToDisplay = "$count";
    
    var filterOneMeasure = {$match: { measure: wochenaufgabe}};
    var filterRegionalschluessel = {$match: {schluessel: {$regex: "^"+location}}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,lengthOfKey]},
    						 			  timestamp: "$timestamp",
    						 			  timestampShort: {$substr: ["$timestamp",0,10]},
    						 			  count: valueToCount,
    						 			  total: "$count",
    						 			  source: "$source"
    						 			  }};	
     var aggregateMeasuresGroup = {$group: { _id: { row: "$schluessel",source:"$source"},
    						 		    count	: {$sum: "$count" },
    						 		    total	: {$sum: "$total" },
    						 		    timestamp:{$last:"$timestamp"},
    						 		    schluessel: {$last:"$schluessel"},
    						 		    timestampShort: {$last:"$timestampShort"}}};
      
    var presort = {$sort: { timestamp:1}};
 
    var aggregateTimeAxisStep2 = {$group: { _id: { row: "$schluessel",col:"$timestampShort"},
    						 		    cell	: {$last: valueToDisplay }}};

     var postsort = {$sort: { _id:1}};
   
    
   
    	
    var query = [filterOneMeasure,
				 filterRegionalschluessel,
    			 aggregateMeasuresProj,
    			 aggregateMeasuresGroup,
    			 presort,
    			 aggregateTimeAxisStep2,
    			 postsort];


    						 		  
    						 		  
	debug.data("query:"+JSON.stringify(query));

    var aggFunc=query;   						
    var items = [];
    async.parallel( [ 
    	function aggregateCollection(callback) {
    		debug.entry("aggregateCollection");
			collection.aggregate(	query
								, (function aggregateCollectionCB(err, data) {
				debug.entry("aggregateCollectionCB");
				// first copy hole table in a 2 dimensional JavaScript map
				// may be here is some performance potential :-)
				if (err) {
					res.set('Content-Type', 'text/html');
					res.end("error"+err);
					console.log("Table Function, Error occured:");
					console.log("Error: "+err);
				}
				items = data;
				callback(err);
			}))}],
			function displayFinalCB (err, results ) {
				bundeslandRow = {};
				debug.entry("displayFinalCB");
				// Initialising JavaScript Map
				//iterate total result array
				// and generate 2 Dimensional Table
				for (i=0;i < items.length;i++) {
			
			
					measure = items[i];
					debug.data("Measure i"+i+JSON.stringify(measure));
					schluessel=measure._id.row;
					datum=measure._id.col;
					cell=parseFloat(measure.cell);
					
					
					
					//generate new Header or Rows and Cell if necessary	
					if (typeof(bundeslandRow[schluessel])=='undefined') {
						
						bundeslandRow[schluessel]={};
						bundeslandRow[schluessel].x = [];
						bundeslandRow[schluessel].y = [];
						bundeslandRow[schluessel].name = getKreisname(schluessel,kreisnamen);
						bundeslandRow[schluessel].type = "scatter";
					}
					bundeslandRow[schluessel].x.push(datum);
					bundeslandRow[schluessel].y.push(cell);
				}
				data = [];
				for (key in bundeslandRow) {
					data.push(bundeslandRow[key]);
				}
				graphLocation = getKreisname(location,kreisnamen);
				
				filenamePlotly = "OSMWA_"+wochenaufgabe+"_"+graphLocation+"_"+lengthOfKey;
				title = "--"
				switch (wochenaufgabe) {
					case "Apotheke": title =  "Anzahl Apotheken in OSM fuer "+graphLocation;
					            break;
					case "AddrWOStreet": title = "Adressen ohne Strasse fuer "+graphLocation;
								break;
					default: title = "--";
				
				}
				var style = {title: title};
				
				var graphOptions = {filename: filenamePlotly, fileopt: "overwrite",layout:style};
				plotly.plot(data, graphOptions, function (err, msg) {
			
    			res.set('Content-Type', 'text/html');
    			if (!err) {
    				page = htmlPage.create("table");
    				page.content='<iframe width="1200" height="600" frameborder="0" seamless="seamless" scrolling="no" src='+msg.url+'.embed?width=1200&height=600"></iframe>';
    				page.footer = "Diese Grafik wird durch den Aufruf dieser Seite aktualisiert, der plot.ly Link kann aber auch unabh&aumlnig genutzt werden.";
    				res.end(page.generatePage());
    			} else {
    				res.end("Fehler von Plotly: "+ JSON.stringify(err));
    			}		
				});
				
			}
			
		)

}

exports.plotValues = function(req,res){
	debug.entry("exports.plotValues");
	var db = configuration.getDB();
	
	var wochenaufgabe = req.param("measure");
	
	if (wochenaufgabe != "Apotheke") {
		res.set('Content-Type', 'text/html');
		res.end("Diese Grafik wird nur von der Wochenaufgabe Apotheke supportet");
	}
    var location = req.param("location");
    if (typeof(location) =='undefined') location="";

    var collection = db.collection('DataCollection');
   

	var kreisnamen = loadDataFromDB.schluesselMapAGS;	 
 
 
    valueToCount = "$count";
   	valueToDisplay = "$count";
    
    var filterOneMeasure = {$match: { measure: wochenaufgabe}};
    var filterRegionalschluessel = {$match: {schluessel: {$regex: "^"+location}}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,location.length]},
    						 			  timestamp: "$timestamp",
    						 			  timestampShort: {$substr: ["$timestamp",0,10]},
    						 			  count: valueToCount,
    						 			  total: "$count",
    						 			  source: "$source",
    						 			  m_name: "$missing.name",
    						 			  m_opening_hours	: "$missing.opening_hours" ,
    						 		      m_phone	:  "$missing.phone" ,
    						 		      m_wheelchair	: "$missing.wheelchair" ,
    						 		      e_fixme	: "$existing.fixme" 
    						 			  }};	
     var aggregateMeasuresGroup = {$group: { _id: { row: "$schluessel",source:"$source"},
    						 		    "m_name"	: {$sum: "$m_name" },
    						 		    "m_opening_hours"	: {$sum: "$m_opening_hours" },
    						 		    "m_phone"	: {$sum: "$m_phone" },
    						 		    "m_wheelchair"	: {$sum: "$m_wheelchair" },
    						 		    "e_fixme"	: {$sum: "$e_fixme" },
    						 		    total	: {$sum: "$total" },
    						 		    timestamp:{$last:"$timestamp"},
    						 		    schluessel: {$last:"$schluessel"},
    						 		    timestampShort: {$last:"$timestampShort"}}};
      
    var presort = {$sort: { timestamp:1}};
 
    var aggregateTimeAxisStep2 = {$group: { _id: { row: "$schluessel",col:"$timestampShort"},
    						 		    "m_name"	: {$last: "$m_name" },
    						 		    "m_opening_hours"	: {$last: "$m_opening_hours" },
    						 		    "m_phone"	: {$last: "$m_phone" },
    						 		    "m_wheelchair"	: {$last: "$m_wheelchair" },
    						 		    "e_fixme"	: {$last: "$e_fixme" },
    						 		    total	: {$last: "$total" }}};

     var postsort = {$sort: { _id:1}};
   
    
   
    	
    var query = [filterOneMeasure,
				 filterRegionalschluessel,
    			 aggregateMeasuresProj,
    			 aggregateMeasuresGroup,
    			 presort,
    			 aggregateTimeAxisStep2,
    			 postsort];


    						 		  
    						 		  
	debug.data("query:"+JSON.stringify(query));

    var aggFunc=query;   						
    var items = [];
    async.parallel( [ 
    	function aggregateCollection(callback) {
    		debug.entry("aggregateCollection");
			collection.aggregate(	query
								, (function aggregateCollectionCB(err, data) {
				debug.entry("aggregateCollectionCB");
				// first copy hole table in a 2 dimensional JavaScript map
				// may be here is some performance potential :-)
				if (err) {
					res.set('Content-Type', 'text/html');
					res.end("error"+err);
					console.log("Table Function, Error occured:");
					console.log("Error:"+err);
					;
				}
				items = data;
				callback(err);
			}))}],
			function displayFinalCB (err, results ) {
				debug.entry("displayFinalCB");
				// Initialising JavaScript Map
				//iterate total result array
				// and generate 2 Dimensional Table

				m_name = {} ;
				m_name.name = "name";
				m_name.type = "bar";
				m_opening_hours = {};
				m_opening_hours.name = "opening_hours";
				m_opening_hours.type = "bar";
				m_phone = {};
				m_phone.name = "phone";
				m_phone.type = "bar";
				m_wheelchair = {};
				m_wheelchair.name = "wheelchair";
				m_wheelchair.type = "bar";
				e_fixme = {};
				e_fixme.name = "fixme";
				e_fixme.type = "bar";
				m_name.x = [] ;
				m_opening_hours.x = [];
				m_phone.x = [];
				m_wheelchair.x = [];
				e_fixme.x = [];
				m_name.y = [] ;
				m_opening_hours.y = [];
				m_phone.y = [];
				m_wheelchair.y = [];
				e_fixme.y = [];
				
				for (i=0;i < items.length;i++) {
			
			
					measure = items[i];
					debug.data("Measure i"+i+JSON.stringify(measure));
					schluessel=measure._id.row;
					datum=measure._id.col;
					m_name.y.push(parseFloat(measure.m_name));
					m_opening_hours.y.push(parseFloat(measure.m_opening_hours));
					m_phone.y.push(parseFloat(measure.m_phone));
					m_wheelchair.y.push(parseFloat(measure.m_wheelchair));
					e_fixme.y.push(parseFloat(measure.e_fixme));
					m_name.x.push(datum);
					m_opening_hours.x.push(datum);
					m_phone.x.push(datum);
					m_wheelchair.x.push(datum);
					e_fixme.x.push(datum);
					//cell=parseFloat(measure.cell);
				}
				data = [];
				data.push(m_name);
				data.push(m_opening_hours);
				data.push(m_phone);
				data.push(m_wheelchair);
				data.push(e_fixme);
				
				debug.data(data);
				
				graphLocation = getKreisname(location,kreisnamen);
				
				filenamePlotly = "OSMWA_"+wochenaufgabe+"_"+graphLocation;
				title = "--"
				switch (wochenaufgabe) {
					case "Apotheke": title =  "Fehlende Apotheken Tags in OSM fuer "+graphLocation;
					            break;
					case "AddrWOStreet": title = "Adressen ohne Strasse fuer "+graphLocation;
								break;
					default: title = "--";
				
				}
				var style = {title: title};
				
				var graphOptions = {filename: filenamePlotly, fileopt: "overwrite",layout:style};
				plotly.plot(data, graphOptions, function (err, msg) {
    				res.set('Content-Type', 'text/html');
    				if (!err) {
    					page = htmlPage.create("table");
    					page.content='<iframe width="1200" height="600" frameborder="0" seamless="seamless" scrolling="no" src='+msg.url+'.embed?width=1200&height=600"></iframe>';
    					page.footer = "Diese Grafik wird durch den Aufruf dieser Seite aktualisiert, der plot.ly Link kann aber auch unabh&aumlnig genutzt werden.";
    					res.end(page.generatePage());
    				} else {
    					res.end("Fehler von Plotly: "+ JSON.stringify(err));
    				}		
				});
				
			}
			
		)

}

