var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('test');
  debug.data = require('debug')('test:data');
  debug.entry = require('debug')('test:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');

var async    = require('async');





exports.plot = function(req,res){
	debug.entry("exports.plot");
	var db = configuration.getDB();
  
    var collection = db.collection('DataCollection');
   

	var kreisnamen = loadDataFromDB.schluesselMapAGS;	 
 
 
    valueToCount = "$count";
   	valueToDisplay = "$count";
    
    var filterOneMeasure = {$match: { measure: "Apotheke"}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,2]},
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
						n = kreisnamen[schluessel].name;
						n = n.replace("ü","ue");
						n = n.replace("ö","ae");
						n = n.replace("ä","ae");
						bundeslandRow[schluessel]={};
						bundeslandRow[schluessel].x = [];
						bundeslandRow[schluessel].y = [];
						bundeslandRow[schluessel].name = n;
						bundeslandRow[schluessel].type = "scatter";
					}
					bundeslandRow[schluessel].x.push(datum);
					bundeslandRow[schluessel].y.push(cell);
				}
				data = [];
				for (key in bundeslandRow) {
					data.push(bundeslandRow[key]);
				}
				
				console.dir(data);
				var graphOptions = {filename: "Apotheke", fileopt: "overwrite"};
				plotly.plot(data, graphOptions, function (err, msg) {
				console.log("Error "+err);
    			console.log("msg" + msg);
    			res.end(JSON.stringify(msg));
				});
				
			}
			
		)

}


