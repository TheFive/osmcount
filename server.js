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
          res.end("There are " + count + " records.");
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
    
    displayAllText =[{$match: { measure: "AddrWOStreet"}},
    						 //{$project: { schluessel: "$schluessel",count: "$count",timestamp: "$timestamp"}},
    						 {$group: { _id : "$_id" , 
    						 		    count: {$sum: "$count"},
    						 		    schluessel: {$first:"$schluessel"},
    						 		    timestamp: {$last:"$timestamp"}}},
    						 {$sort: { schluessel:1,timestamp:1}},
    						 
    						
    						];
    displayBundeslandText = [{$match: { measure: "AddrWOStreet"}},
    						 {$project: { schluessel: { $substr: ["$schluessel",0,2]},
    						 			  countintern: "$count",
    						 			  timestamp: "$timestamp"}},
    						 {$group: { _id: { schluessel: "$schluessel",timestamp:"$timestamp"},
    						 		    count: {$sum: "$countintern" }}},
    						 {$sort: { schluessel:1,timestamp:1}},
    						 
    						
    						];
    // Bitte Checken, Parameter geht noch nicht
    var aggFunc=displayAllText;   						
    if (req.param("all")=="NO"){
    	aggFunc=displayBundeslandText;
    } 
    
    // change the Collection to a HTML Table
    collection.aggregate(	displayBundeslandText
    
    
    						, (function(err, items) {
    	// first copy hole table in a 2 dimensional JavaScript map
    	// may be here is some performance potential :-)
    	if (err) {
    		res.end("error");
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
			console.dir(measure);
			
			//generate new Header or Rows and Cell if necessary	
			if (header.indexOf(measure.timestamp)<0) {
				header.push(measure.timestamp);
			}
			if (firstColumn.indexOf(measure.schluessel)<0) {
				firstColumn.push(measure.schluessel);
			}
			if (!Array.isArray(table[measure.schluessel])) {
				table[measure.schluessel]=[];
			}
			
			table[measure.schluessel][measure.timestamp]=parseInt(measure.count);
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
		text = "<html><body>"+beforetext+"<table border=\"1\">\n" + tableheader + tablebody + "</table></body></head>";
		res.end(text);
	}));
	});
	
 
app.use('/', express.static(__dirname));
app.get('/*', function(req, res) {
    res.status(404).sendFile(__dirname + '/error.html');
});
app.listen(configuration.serverport);

console.log("Server has started and is listening to localhost:"+configuration.serverport);
	