var loadDataFromDB = require('./LoadDataFromDB')
var importCSV=require('./ImportCSV');


exports.count = function(req,res){
    db = res.db;
    collectionName = 'DataCollection';
     if(typeof(req.param("collection"))!='undefined') {
     	collectionName = req.param("collection");
     }
  
    // Fetch the collection test
    var collection = db.collection(collectionName);
    collection.count(function(err, count) {
    	res.set('Content-Type', 'text/html');
    	if(err) {
    		res.end(err);
    	} else {
    		res.end("There are " + count + " records in Collection "+ collectionName);
    	}
    });  
}

exports.importCSV = function(req,res){
    db = res.db;
  
    // Fetch the collection test
    defJSON = { measure: "AddrWOStreet",  count: 0};
    console.log("Count hat den typ"+typeof(defJSON["count"]));
    importCSV.readCSV(db,  defJSON,"allLF-UTF8.csv");
    res.end("File IMported Probably");
	}
	
	
function generateLink(text, basis, param1,param2,param3)
{
  result ="";
  result += text;
  link = basis;
  sep="?";
  if (param1){
  	link+= sep+param1;
  	sep="&";
  }
  if (param2) {
  	link+= sep+param2;
  	sep="&";
  }
  if (param3) {
  	link+= sep+param3;
  	sep="&";
  }
  result = "<a href="+link+">"+result + "</a>";
  return result;
}


exports.table = function(req,res){
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
    periode="day";
    if(req.param("period")=="year") {
    	lengthOfTime=4;
    	periode="year";
    }
    if(req.param("period")=="month") {
    	lengthOfTime=7;
    	periode="month";
    }
    if(req.param("period")=="day") {
    	lengthOfTime=10;
    	periode="day";
    }
    
    basisLink = "./table.html";
    paramTime = "period="+periode;
    paramMeasure = "measure="+displayMeasure;
    paramLength = "lok+"+lengthOfKey;
    
    beforeText= "<h1>"+"Tabelle für Messung "+displayMeasure+"</h1>";
    beforeText+= "Dargestellte Periode "+periode+"<br>";
    beforeText+= generateLink("[Year]",basisLink,paramLength,"period=year",paramMeasure);
    beforeText+= generateLink("[month]",basisLink,paramLength,"period=month",paramMeasure);
    beforeText+= generateLink("[day]",basisLink,paramLength,"period=day",paramMeasure); 
    beforeText+="<br>";  
    beforeText+= "Schlüssellänge = "+lengthOfKey+"<br>";
    beforeText+= generateLink("[Bundesländer]",basisLink,"lok=2",paramTime,paramMeasure);
    beforeText+= generateLink("[Middle]",basisLink,"lok=3",paramTime,paramMeasure);
    beforeText+= generateLink("[detail]",basisLink,"lok=10",paramTime,paramMeasure);
    
    
    
    
    
   
    	
    displayText = [{$match: { measure: displayMeasure}},
    						 {$project: { schluessel: "$schluessel",
    						 			  timestamp: {$substr: ["$timestamp",0,lengthOfTime]},
    						 			  count: "$count",
    						 			  schluessel2: "$schluessel",
    						 			  timestamp2: "$timestamp",
    						 			  }},
    						 {$group: { _id: { row: "$schluessel",col:"$timestamp"},
    						 		    count	: {$last: "$count" },
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
 	kreisnamen = loadDataFromDB.schluessel();
 	
    
    //console.log(JSON.stringify(displayText));
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
		
		
		
		tableheader = "<th>Regioschlüssel</th><th>Name</th>";
		for (i=0;i<header.length;i++) {
			tableheader +="<th>"+header[i]+"</th>";
		}
		tableheader = "<tr>"+tableheader + "</tr>";
		tablebody="";
		{
			for (i=0;i<firstColumn.length;i++)
			{
				schluessel = firstColumn[i];	
				schluesselText=schluessel;
				
				if (typeof(kreisnamen[schluessel])!= 'undefined') {
					schluesselText = kreisnamen[schluessel];
				}
				var row = "<td>"+schluessel+"</td>"+"<td>"+schluesselText+"</td>";
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
			text = "<html><body>"+beforeText+"<table border=\"1\">\n" + tableheader + tablebody + "</table></body></html>";
			res.set('Content-Type', 'text/html');
			res.end(text);
		};
	})
)
}