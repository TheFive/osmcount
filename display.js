var loadDataFromDB = require('./LoadDataFromDB')
var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');
  debug.data = require('debug')('display:data');
  debug.entry = require('debug')('display:entry');
  
var path     = require('path');
var fs       = require("fs");
var numeral  = require('numeral');
var de       = require('numeral/languages/de');
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;


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


exports.count = function(req,res){
	debug.entry("exports.count");
    var db = res.db;
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

exports.object = function(req,res) {
	debug.entry("exports.object");
	var db = res.db;
   
	var collectionName = req.param("collection");
	var objid = req.param("id");
	var object=ObjectID(objid);
   
    db.collection(collectionName).findOne ({_id:object},function (err, obj) {
    	if (err) {
    		var text = "Display Object "+ objid + " in Collection "+collectionName;
    		res.end(err);
    	} else {
    		text = "";
    		for (key in obj) {
    			if (key != "_id") {
    				text += "<tr><td>"+key+"</td><td>"+obj[key]+"</td></tr>";
    			}
    		}	
    		text = tableCSSStyle+"<body><h1>Data Inspector</h1><table>"+text+"</table></body>";
    		
    		res.end(text );
    	}
    })
}

exports.importCSV = function(req,res){
	debug.entry("exports.importCSV");
    var db = res.db;
  
    // Fetch the collection test
    var importDir = path.resolve(__dirname, "import")
    var listOfCSV=fs.readdirSync(importDir);
    var text = "";
    for (i=0;i<listOfCSV.length;i++)
    {
    	var filename=listOfCSV[i];
    	var year = filename.substring(0,4);
    	var month = filename.substring(5,5+2);
    	var day = filename.substring(8,8+2);
    	var date = new Date(year,month-1,day);
    	debug("Datum"+date+"("+year+")("+month+")("+day+")");
    	date.setTime( date.getTime() - date.getTimezoneOffset()*60*1000 );
    	defJSON = { measure: "AddrWOStreet",  
    					count: 0,
    					timestamp:date,
    					execdate:date,
    					source:filename};
    	var filenameLong=path.resolve(importDir,filename);
    	importCSV.readCSV(filenameLong,db,  defJSON);
    	text += "Import: "+filename+"->"+date+"\n"
    }
    text += "Files imported";
    res.end(text);
}
	
	
function generateLink(text, basis, param1,param2,param3,param4)
{
	debug.entry("generateLink");
  var result = text;
  var link = basis;
  var sep="?";
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
  return "<a href="+link+">"+result + "</a>";
}



exports.table = function(req,res){
	debug.entry("exports.table");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    

	var kreisnamen = loadDataFromDB.schluesselMap;
	numeral.language('de', {
        delimiters: {
            thousands: '.',
            decimal: ','
        },
        abbreviations: {
            thousand: 'k',
            million: 'm',
            billion: 'b',
            trillion: 't'
        },
        ordinal: function (number) {
            return '.';
        },
        currency: {
            symbol: '€'
        }
    });
	numeral.language('de');


 
    // Parse html parameters
    var displayMeasure="AddrWOStreet";    
    if(typeof(req.param("measure"))!='undefined') {
    	displayMeasure=req.param("measure");
    }
    
    // length for the Regioschluessel
    var lengthOfKey=2;
    if (typeof(req.param("lok")) != 'undefined' && req.param("lok").parseInt != 'NaN') {
     	lengthOfKey=parseInt(req.param("lok"));
    }
    
    // length of Timestamp
    var lengthOfTime=7;
    var periode="Monat";
    if(req.param("period")=="Jahr") {
    	lengthOfTime=4;
    	periode="Jahr";
    }
    if(req.param("period")==" Monat") {
    	lengthOfTime=7;
    	periode="Monat";
    }
    if(req.param("period")=="Tag") {
    	lengthOfTime=10;
    	periode="Tag";
    }
    var startWith="";
    var location=startWith;
    var locationType="-";
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
    
    
    var basisLink = "./table.html";
    var paramTime = "period="+periode;
    var paramMeasure = "measure="+displayMeasure;
    var paramLength = "lok="+lengthOfKey;
    var paramLocation = "location="+startWith;
    
    beforeText= "<h1> Messergebnisse </h1>";
    var filterText = "";
    if (startWith!="") {
    	filterText+= location+" ("+startWith+","+locationType+")";
    	
    } else {
    	filterText +="kein Filter";
    }
    periodenSwitch = generateLink("[Jahr]",basisLink,paramLength,"period=Jahr",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[Monat]",basisLink,paramLength,"period=Monat",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[Tag]",basisLink,paramLength,"period=Tag",paramMeasure,paramLocation); 
    lokSwitch ="";   
    
    if (lengthOfKey >2) lokSwitch += generateLink("weniger",basisLink,"lok="+(lengthOfKey-1),paramTime,paramMeasure,paramLocation)+" ";
    if (lengthOfKey <12) lokSwitch+= generateLink("mehr",basisLink,"lok="+(lengthOfKey+1),paramTime,paramMeasure,paramLocation);

    var filterTable = "<tr><td>Messung</td><td><b>"+displayMeasure+"</b></td><td></td></tr>"
    filterTable += "<tr><td>Filter</td><td><b>"+filterText+"</b></td><td>"+generateLink("[Bundesländer]",basisLink,"lok=2",paramTime,paramMeasure)+"</td></tr>"
    filterTable += "<tr><td>Periode</td><td><b>"+periode+"</B></td><td>"+periodenSwitch+"</td></tr>"
    filterTable += "<tr><td>Schlüssellänge</td><td><b>"+lengthOfKey+"</b></td><td>"+lokSwitch+"</td></tr>"
    
	filterTable = "<table>"+filterTable+"</table><br><br>";
	beforeText += filterTable;
   
 
    
    
    var filterOneMeasure = {$match: { measure: displayMeasure}};
    var filterRegionalschluessel = {$match: {schluessel: {$regex: "^"+startWith}}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,lengthOfKey]},
    						 			  timestamp: "$timestamp",
    						 			  timestampShort: {$substr: ["$timestamp",0,lengthOfTime]},
    						 			  count: "$count",
    						 			  source: "$source"
    						 			  }};	
     var aggregateMeasuresGroup = {$group: { _id: { row: "$schluessel",source:"$source"},
    						 		    count	: {$sum: "$count" },
    						 		    timestamp:{$last:"$timestamp"},
    						 		    schluessel: {$last:"$schluessel"},
    						 		    timestampShort: {$last:"$timestampShort"}}};
      
    var presort = {$sort: { timestamp:1}};
 
    var aggregateTimeAxisStep2 = {$group: { _id: { row: "$schluessel",col:"$timestampShort"},
    						 		    cell	: {$last: "$count" }}};

    var sort = {$sort: { _id:1}};
    
    
   
    	
    var query = [filterOneMeasure,
    			 filterRegionalschluessel,
    			 aggregateMeasuresProj,
    			 aggregateMeasuresGroup,
    			 presort,
    			 aggregateTimeAxisStep2,
    			 sort];


    var aggFunc=query;   						
 	var openQueries=0;
    var items = [];
    async.parallel( [ function(callback) {
   				 collection.aggregate(	query
										, (function(err, data) {
						// first copy hole table in a 2 dimensional JavaScript map
						// may be here is some performance potential :-)
						if (err) {
							res.end("error"+err);
							console.log("Table Function, Error occured:");
							console.log(err);
							;
						}
						items = data;
						callback();
					}))},
					function (callback) {
					
						db = res.db;
						collectionName = 'WorkerQueue';
						
						// Fetch the collection test
						var collection = db.collection(collectionName);
						collection.count({status:"open"},function(err, count) {
							openQueries=count;
						});  
						callback();

					
					}],
					function (err, results ) {
    	// Initialising JavaScript Map
    	var header = [];
		var firstColumn = [];
		var table =[]; 
		
		//iterate total result array
		for (i=0;i < items.length;i++) {
			
			
			measure = items[i];
			debug.data("Measure i"+i+JSON.stringify(measure));
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
		header.sort();
		
		
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
			
			pagefooter = "<p> Offene Queries "+openQueries+"</p>";
			pagefooter += "<p><h2>MongoDB Aggregat Funktion:</h2>";
			pagefooter += "<pre>"+JSON.stringify(query,null,' ')+"</pre></p>";
			text = "<html>"+tableCSSStyle+"<body>"+beforeText+"<table border=\"1\">\n" + tableheader + tablebody + "</table>"+pagefooter+"</body></html>";
			res.set('Content-Type', 'text/html');
			res.end(text);
		};
	})

}


