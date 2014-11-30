//require node modules (see package.json)
var express = require('express');
var MongoClient = require('mongodb').MongoClient;
var format = require('util').format;


// require own modules
var importCSV=require('./ImportCSV');



var app = express();



// Reading the configuration file

var fs, configurationFile;

configurationFile = 'configuration.json';
fs = require('fs');

var configuration = JSON.parse(
  fs.readFileSync(configurationFile)
);

// Log the information from the file
console.log(configuration);



var mongodb;

var mongodbConnectStr ='mongodb://'
                   + configuration.username + ':'
                   + configuration.password + '@'
                   + configuration.database;
                   
console.log("connect:"+mongodbConnectStr);

MongoClient.connect(mongodbConnectStr, function(err, db) {
  if (err) throw err;
  mongodb = db;
  console.log("Connected to Database mosmcount");
  })




app.use(function(req, res, next){
    console.log(req.url);
    res.db = mongodb;
    next();
});

// Zeige eine Count Seite
app.use('/count.html', function(req,res){
    db = res.db;
  
    // Fetch the collection test
    var collection = db.collection('DataCollection');
    collection.count(function(err, count) {
    	res.set('Content-Type', 'text/html');
    	if(err) {
    		res.end(err);
    	} else {
    		res.end("There are " + count + " records.");
    	}
    });  
})


app.use('/import.html', function(req,res){
    db = res.db;
  
    // Fetch the collection test
    defJSON = { measure: "AddrWOStreet",  count: 0};
    console.log("Count hat den typ"+typeof(defJSON["count"]));
    importCSV.readCSV(db,  defJSON,"allLF-UTF8.csv");
    res.end("File IMported Probably");
	})


// Zeige eine Tabelle Seite
app.use('/table.html', function(req,res){
	db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    


    // Parse html parameters
    displayMeasure="AddrWOStreet";    
    if(typeof(req.param("measure"))!='undefined') {
    	displayMeasure=req.param("measure");
    }
    
    // length for the Regioschluessel
    lengthOfKey=2;
    if(typeof(req.param("lok")!='undefined')) {
    	lengthOfKey=parseInt(req.param("lok"));
    }
    
    // length of Timestamp
    lengthOfTime=10;
    if(req.param("period")=="year") {
    	lengthOfTime=4;
    }
    if(req.param("period")=="month") {
    	lengthOfTime=7;
    }
    if(req.param("period")=="day") {
    	lengthOfTime=10;
    }
    
    
    
   
    	
    displayText = [{$match: { measure: displayMeasure}},
    						 {$project: { schluessel: "$schluessel",
    						 			  timestamp: {$substr: ["$timestamp",0,lengthOfTime]},
    						 			  count: "$count",
    						 			  schluessel2: "$schluessel",
    						 			  timestamp2: "$timestamp",
    						 			  }},
    						 {$group: { _id: { row: "$schluessel",col:"$timestamp"},
    						 		    count: {$last: "$count" },
    						 		    schluessel:{$last: "$schluessel2"},
    						 		    timestamp:{$last:"$timestamp2"}}},
    						 {$project: { schluessel: { $substr: ["$schluessel",0,lengthOfKey]},
    						 			  time: {$substr: ["$timestamp",0,lengthOfTime]},
    						 			  count: "$count"
    						 			  }},
    						 {$group: { _id: { row: "$schluessel",col:"$time"},
    						 		    cell: {$sum: "$count" }}},
    						 {$sort: { _id:1}},
    						 
    						
    						];
    // Bitte Checken, Parameter geht noch nicht
    var aggFunc=displayText;   						
 
    
    console.log(JSON.stringify(displayText));
    collection.aggregate(	displayText
    
    
    						, (function(err, items) {
    	// first copy hole table in a 2 dimensional JavaScript map
    	// may be here is some performance potential :-)
    	if (err) {
    		res.end("error"+err);
    		console.log(err);
    		return;
    	}
    	// Initialising JavaScript Map
    	header = [];
		firstColumn = [];
		table =[]; 
		
		beforetext= "Table contains " + items.length+"  items";
		
		//iterate total result array
		for (i=0;i < items.length;i++) {
			
		
			measure = items[i];
			//console.dir(measure);
			
			row=measure._id.row;
			col=measure._id.col;
			
			if (typeof(row)=='undefined' || typeof(col) == 'undefined') {
				console.log("row or col undefined"+row+col);
			}
			
			//generate new Header or Rows and Cell if necessary	
			if (header.indexOf(col)<0) {
				header.push(col);
			}
			if (firstColumn.indexOf(row)<0) {
				firstColumn.push(row);
			}
			if (!Array.isArray(table[row])) {
				table[row]=[];
			}
			
			table[row][col]=parseInt(measure.cell);
			//console.log(measure.schluessel+","+measure.timestamp+","+cell);
			//console.log("-->"+table[measure.schluessel][measure.timestamp]);
		}
		
		tableheader = "<th>Regioschlüssel</th>";
		for (i=0;i<header.length;i++) {
			tableheader +="<th>"+header[i]+"</th>";
		}
		tableheader = "<tr>"+tableheader + "</tr>";
		tablebody="";
		for (i=0;i<firstColumn.length;i++)
		{
			schluessel = firstColumn[i];
			var row = "<td>"+schluessel+"</td>";
			for (z=0;z<header.length;z++) {
				timestamp=header[z];
				//console.log(schluessel+","+timestamp+"->"+table[schluessel][timestamp]);
				var cell;
				var content=table[schluessel][timestamp];
				if (typeof(content) == "undefined") {
					cell ="-";
				} else {
					cell = content;
				}
					
				row += "<td>"+cell+"</td>";
			}
			row = "<tr>"+row+"</tr>";
			tablebody += row;
		}
		tableheader += "</tr>";
		tablebody +="</tr>"	;
		tablebody += "";
		text = "<html><body>"+beforetext+"<table border=\"1\">\n" + tableheader + tablebody + "</table></body></html>";
		res.set('Content-Type', 'text/html');
		res.end(text);
	}));
	});
	
 
app.use('/', express.static(__dirname));
app.get('/*', function(req, res) {
    res.status(404).sendFile(__dirname + '/error.html');
});
app.listen(configuration.serverport);

console.log("Server has started and is listening to localhost:"+configuration.serverport);
	