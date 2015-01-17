var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('test');
  debug.data = require('debug')('test:data');
  debug.entry = require('debug')('test:entry');
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
					console.log(err);x
					;
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
			
    			page = htmlPage.create("tabelle");
    			page.content='<iframe width="1200" height="600" frameborder="0" seamless="seamless" scrolling="no" src='+msg.url+'.embed?width=1200&height=600"></iframe>';
    			page.footer = "Diese Grafik wird durch den Aufruf dieser Seite aktualisiert, der plot.ly Link kann aber auch unabh&aumlnig genutzt werden.";
    			res.set('Content-Type', 'text/html');
    			if (!err) {
    				res.end(page.generatePage());
    			} else {
    				res.end(err);
    			}		
				});
				
			}
			
		)

}


