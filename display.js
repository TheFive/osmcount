var loadDataFromDB = require('./LoadDataFromDB')
var importCSV=require('./ImportCSV');
var debug   = require('debug')('display');
var path    = require('path');
var fs      = require("fs");
var numeral = require('numeral');
var de = require('numeral/languages/de');


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
    importDir = path.resolve(__dirname, "import")
    listOfCSV=fs.readdirSync(importDir);
    text = "";
    for (i=0;i<listOfCSV.length;i++)
    {
    	filename=listOfCSV[i];
    	year = filename.substring(0,4);
    	month = filename.substring(5,5+2);
    	day = filename.substring(8,8+2);
    	date = new Date(year,month-1,day);
    	debug("Datum"+date+"("+year+")("+month+")("+day+")");
    	date.setTime( date.getTime() - date.getTimezoneOffset()*60*1000 );
    	defJSON = { measure: "AddrWOStreet",  count: 0,timestamp:date,execdate:date};
    	filename=path.resolve(importDir,filename);
    	importCSV.readCSV(filename,db,  defJSON);
    	text += "Import: "+filename+"->"+date+"\n"
    }
    text += "Files imported";
    res.end(text);
}
	
	
function generateLink(text, basis, param1,param2,param3,param4)
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
  if (param4) {
  	link+= sep+param4;
  	sep="&";
  }
  result = "<a href="+link+">"+result + "</a>";
  return result;
}

var tableCSSStyle = '<head>\
<style>\
table, th, td {\
    border: 1px solid black;\
    border-collapse: collapse;\
    font-family: Verdana;\
	font-size: 0.9em;\
	text-align: center;\
	padding: 3px;\
}\
th { \
background-color: #999; \
color: #fff; \
border: 1px solid #fff; \
} \
</style>\
</head>'

exports.table = function(req,res){
	db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    

	kreisnamen = loadDataFromDB.schluesselMap;
	numeral.language('de',de);
	numeral.language('de');


 
    // Parse html parameters
    displayMeasure="AddrWOStreet";    
    if(typeof(req.param("measure"))!='undefined') {
    	displayMeasure=req.param("measure");
    }
    
    // length for the Regioschluessel
    lengthOfKey=2;
    if (typeof(req.param("lok")) != 'undefined' && req.param("lok").parseInt != 'NaN') {
     	lengthOfKey=parseInt(req.param("lok"));
    }
    
    // length of Timestamp
    lengthOfTime=7;
    periode="month";
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
    startWith="";
    location=startWith;
    locationType="-";
    if(typeof(req.param("location"))!='undefined') {
    	startWith=req.param("location");
    	location=startWith;
    	locationType="-";
    	v = kreisnamen[startWith];
    	if (typeof(v)!='undefined') {
    		location=v.name;
    		locationType=v.typ;
    	}
    }
    
    
    basisLink = "./table.html";
    paramTime = "period="+periode;
    paramMeasure = "measure="+displayMeasure;
    paramLength = "lok="+lengthOfKey;
    paramLocation = "location="+startWith;
    
    beforeText= "<h1> Messergebnisse </h1>";
    filterText = "";
    if (startWith!="") {
    	filterText+= location+" ("+startWith+","+locationType+")";
    	
    } else {
    	filterText +="kein Filter";
    }
    periodenSwitch = generateLink("[Year]",basisLink,paramLength,"period=year",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[month]",basisLink,paramLength,"period=month",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[day]",basisLink,paramLength,"period=day",paramMeasure,paramLocation); 
    lokSwitch ="";   
    
    if (lengthOfKey >2) lokSwitch += generateLink("weniger",basisLink,"lok="+(lengthOfKey-1),paramTime,paramMeasure,paramLocation)+" ";
    if (lengthOfKey <12) lokSwitch+= generateLink("mehr",basisLink,"lok="+(lengthOfKey+1),paramTime,paramMeasure,paramLocation);

    filterTable = "<tr><td>Messung</td><td><b>"+displayMeasure+"</b></td><td></td></tr>"
    filterTable += "<tr><td>Filter</td><td><b>"+filterText+"</b></td><td>"+generateLink("[Bundesländer]",basisLink,"lok=2",paramTime,paramMeasure)+"</td></tr>"
    filterTable += "<tr><td>Periode</td><td><b>"+periode+"</B></td><td>"+periodenSwitch+"</td></tr>"
    filterTable += "<tr><td>Schlüssellänge</td><td><b>"+lengthOfKey+"</b></td><td>"+lokSwitch+"</td></tr>"
    
	filterTable = "<table>"+filterTable+"</table><br><br>";
	beforeText += filterTable;
   
 
    
    
    
    
    
   
    	
    var query = [{$match: { measure: displayMeasure}},
    						 {$project: { schluessel: "$schluessel",
    						 			  timestamp: {$substr: ["$timestamp",0,lengthOfTime]},
    						 			  count: "$count",
    						 			  schluessel2: "$schluessel",
    						 			  timestamp2: "$timestamp",
    						 			  }},
    						 {$match: {schluessel: {$regex: "^"+startWith}}},
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


    var aggFunc=query;   						
 	
    
   
    collection.aggregate(	query
    
    
    						, (function(err, items) {
    	// first copy hole table in a 2 dimensional JavaScript map
    	// may be here is some performance potential :-)
    	if (err) {
    		res.end("error"+err);
    		console.log("Table Function, Error occured:");
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
		
			row=measure._id.row;
			col=measure._id.col;
			
			if (typeof(row)=='undefined' || typeof(col) == 'undefined') {
				debug("row or col undefined"+row+col);
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
			
		}
		
		
		
		tableheader = "<th>Regioschlüssel</th><th>Name</th><th>Admin Level</th>";
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
				schluesselTyp="-";
				value = kreisnamen[schluessel];
				
				if (typeof(value)!= 'undefined') {
					schluesselText = value.name;
					schluesselTyp = value.typ;
				}
				schluesselLink = generateLink(schluessel,basisLink,"lok="+(lengthOfKey+1),paramTime,paramMeasure,"location="+schluessel);
 
				var row = "<th>"+schluesselLink+"</th>"+"<td>"+schluesselText+"</td>"+"<td>"+schluesselTyp+"</td>";
				for (z=0;z<header.length;z++) {
					timestamp=header[z];
					
					var cell;
					var content=table[schluessel][timestamp];
					if (typeof(content) == "undefined") {
						cell ="-";
					} else {
						cell = numeral(content).format();
					}
					row += "<td>"+cell+"</td>";
				}
				row = "<tr>"+row+"</tr>";
				tablebody += row;
			}
			tableheader += "</tr>";
			tablebody +="</tr>"	;
			tablebody += "";
			pagefooter = JSON.stringify(query);
			text = "<html>"+tableCSSStyle+"<body>"+beforeText+"<table border=\"1\">\n" + tableheader + tablebody + "</table>"+pagefooter+"</body></html>";
			res.set('Content-Type', 'text/html');
			res.end(text);
		};
	})
)
}