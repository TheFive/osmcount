var loadDataFromDB = require('./LoadDataFromDB')
var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');
  debug.data = require('debug')('display:data');
  debug.entry = require('debug')('display:entry');
  
var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;
var util     = require('./util.js');


var apothekenSoll2013 = {
"08": 2639 ,
"09": 3304 ,
"11": 858 ,
"12": 576 ,
"04": 152 ,
"02": 432 ,
"06": 1546, 
"13": 410 ,
"03": 2014, 
"05": 2393 + 2077, 
"07": 1065 ,
"10": 316 ,
"14": 996 ,
"15": 615 ,
"01": 706 ,
"16": 563 };


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
	
exports.importApotheken = function(req,res) {
	debug.entry("importApotheken");
    var db = res.db;
  
    importCSV.importApothekenVorgabe(db);
    text = "Importiert Apotheken";
    text += "Files imported";
    res.end(text);
}
	
function generateLink(text, basis, param1,param2,param3,param4)
{
  debug.entry("generateLink");
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
  if (text == "") {
  	return link;
  } else {
  	return "<a href="+link+">"+text + "</a>";
  }
}

function setParams(req,param) {
    // Parse html parameters
    param.measure="AddrWOStreet";    
    if(typeof(req.param("measure"))!='undefined') {
    	param.measure=req.param("measure");
    }
    param.sort ="";
    if(typeof(req.param("sort"))!='undefined') {
    	param.sort=req.param("sort");
    }
    param.lengthOfKey = 2;
    if(typeof(req.param("lok"))!="undefined") {
    	param.lengthOfKey=parseInt(req.param("lok"));
    }
    
    param.lengthOfTime=7;
    param.periode="Monat";
    if(req.param("period")=="Jahr") {
    	param.lengthOfTime=4;
    	param.periode="Jahr";
    }   
    if(req.param("period")==" Monat") {
    	param.lengthOfTime=7;
    	param.periode="Monat";
    }
    if(req.param("period")=="Tag") {
    	param.lengthOfTime=10;
    	param.periode="Tag";
    }
   	param.locationStartWith ="";
    param.location="";
    param.locationType="-";
    if(typeof(req.param("location"))!='undefined') {
    	param.location=req.param("location");
    	param.locationStartWith = param.location;
    	param.locationType="-";
    }
    

}


exports.table = function(req,res){
	debug.entry("exports.table");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    var collectionTarget = db.collection('DataTarget');
    

	var kreisnamen; 
	
	numeral = util.numeral;

	param = {};
    setParams(req,param);
	v = kreisnamen[param.locationStartWith];
	if (typeof(v)!='undefined') {
		param.location=v.name;
		param.locationType=v.typ;
	}
    
    if (param.measure) = "Apotheke" {
    		kreisnamen = loadDataFromDB.schluesselMapAGS;
    }
    if (param.measure) = "AddrWOStreet" {
    		kreisnamen = loadDataFromDB.schluesselMapRegio;
    }
    
    var basisLink = "./table.html";
    var paramTime = "period="+param.periode;
    var paramMeasure = "measure="+param.measure;
    var paramLength = "lok="+param.lengthOfKey;
    var paramLocation = "location="+param.locationStartWith;
    
    beforeText= "<h1> OSM Count </h1>";
    var filterText = "";
    if (param.locationStartWith!="") {
    	filterText+= param.location+" ("+param.locationStartWith+","+param.locationType+")";
    	
    } else {
    	filterText +="kein Filter";
    }
    periodenSwitch = generateLink("[Jahr]",basisLink,paramLength,"period=Jahr",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[Monat]",basisLink,paramLength,"period=Monat",paramMeasure,paramLocation);
    periodenSwitch+= generateLink("[Tag]",basisLink,paramLength,"period=Tag",paramMeasure,paramLocation); 
    filterSwitch = generateLink("[AddrWOStreet]",basisLink,"lok=2","period=month","measure=AddrWOStreet");
    filterSwitch += generateLink("[Apotheke]",basisLink,"lok=2","period=month","measure=Apotheke");
    
    lokSwitch ="";   
    
    if (param.lengthOfKey >2) lokSwitch += generateLink("weniger",basisLink,"lok="+(param.lengthOfKey-1),paramTime,paramMeasure,paramLocation)+" ";
    if (param.lengthOfKey <12) lokSwitch+= generateLink("mehr",basisLink,"lok="+(param.lengthOfKey+1),paramTime,paramMeasure,paramLocation);

    var filterTable = "<tr><td>Messung</td><td><b>"+param.measure+"</b></td><td>"+filterSwitch+"</td></tr>"
    filterTable += "<tr><td>Filter</td><td><b>"+filterText+"</b></td><td>"+generateLink("[Bundesländer]",basisLink,"lok=2",paramTime,paramMeasure)+"</td></tr>"
    filterTable += "<tr><td>Periode</td><td><b>"+param.periode+"</B></td><td>"+periodenSwitch+"</td></tr>"
    filterTable += "<tr><td>Schlüssellänge</td><td><b>"+param.lengthOfKey+"</b></td><td>"+lokSwitch+"</td></tr>"
    
	filterTable = "<table>"+filterTable+"</table><br><br>";
	beforeText += filterTable;
   
 
    
    
    var filterOneMeasure = {$match: { measure: param.measure}};
    var filterRegionalschluessel = {$match: {schluessel: {$regex: "^"+param.locationStartWith}}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
    						 			  timestamp: "$timestamp",
    						 			  timestampShort: {$substr: ["$timestamp",0,param.lengthOfTime]},
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

	var queryVorgabe = [
				filterOneMeasure,
				filterRegionalschluessel,
				{$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
    						 			  vorgabe: "$apothekenVorgabe"
    						 			  }},
    			{$group: { _id:  "$schluessel",
    						 vorgabe	: {$sum: "$vorgabe" },
    						 		  }}];
    						 		  
    						 		  
	debug.data("query:"+JSON.stringify(query));
	debug.data("queryVorgabe:"+JSON.stringify(queryVorgabe));

    var aggFunc=query;   						
 	var openQueries=0;
    var items = [];
    var vorgabe = {};
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
					function(callback) {
   					 collectionTarget.aggregate(	queryVorgabe
										, (function(err, data) {
						if (err) {
							res.end("error"+err);
							console.log("Table Function, Error occured:");
							console.log(err);
							;
						}
						console.log("Data Lengh "+data.length);
						for (i = 0;i<data.length;i++)
						{
							console.dir(data[i]);
							schluessel = data[i]._id;
							value = data[i].vorgabe;
							console.log(schluessel + ","+value);
							vorgabe [schluessel]=value;
						}
						console.dir("konverted");
						console.dir(vorgabe);
						callback();
					}))},
					function (callback) {
					
						db = res.db;
						collectionName = 'WorkerQueue';
						
						// Fetch the collection test
						var collection = db.collection(collectionName);
						collection.count({status:"open",measure:param.measure},function(err, count) {
							openQueries=count;
							callback();
						});  
						

					
					}],
					function (err, results ) {
    	// Initialising JavaScript Map
    	var header = [];
		var firstColumn = [];
		var table =[]; 
		var format=[];
		
		//iterate total result array
		// and generate 2 Dimensional Table
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

		
		// Extend two dimensional Table
		displayVorgabe = ((param.lengthOfKey > 1)&&(param.measure=="Apotheke"));
		

		if (displayVorgabe) {
			header.unshift("Vorgabe");
		}
		
		header.unshift("Admin Level");
		header.unshift("Name");
		header.unshift("Regioschlüssel");
		format["Regioschlüssel"]= {};
		format["Regioschlüssel"].generateLink = function(value) {
			return generateLink("",basisLink,"lok="+(param.lengthOfKey+1),paramTime,paramMeasure,"location="+value);
		};
		
		if (displayVorgabe) {
			header.push("Diff");
			format["Diff"]={};
			format["Diff"].format='0%';
			
		}
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
			table[schluessel]["Regioschlüssel"]=schluessel;
			table[schluessel]["Name"]=schluesselText;
			table[schluessel]["Admin Level"]=schluesselTyp;
			if (displayVorgabe){ 
				expectation = vorgabe[schluessel];
				table[schluessel]["Vorgabe"]=expectation;
				lastValue = table[schluessel][header[header.length-2]];
				table[schluessel]["Diff"]=lastValue/expectation;
			}
		}
		tableheader = "";
		for (i=0;i<header.length;i++) {
			tableheader +="<th>"+header[i]+"</th>";
		}
		if (header.indexOf(param.sort>=0)) {
			firstColumn.sort( function(a,b) {return table[b][param.sort]-table[a][param.sort]});
		}
		tableheader = "<tr>"+tableheader + "</tr>";
		tablebody="";
		{
			for (i=0;i<firstColumn.length;i++)
			{
				row = firstColumn[i];
				tablerow = "";
				
				for (z=0;z<header.length;z++) {
					col=header[z];
					
					var cell;
					var content=table[row][col];
					var f = (format[col]) ? format[col].format: null;
					glink = (format[col]) ? format[col].generateLink:null;
					
					if (typeof(content) == "undefined") {
						cell ="-";
					} else {
						if (f) {
							cell = numeral(content).format(f);
						} else if (typeof(content)=='number') {
							cell = numeral(content).format();
						} else {
						    cell = content; // numeral(content).format();
						}
					}
					if (glink) {
						cell = '<a href="'+glink(cell)+'">'+cell+'</a>';
					}
					tablerow += "<td>"+cell+"</td>";
				}
				tablerow = "<tr>"+tablerow+"</tr>";
				tablebody += tablerow;
			}
			
			pagefooter = "<p> Offene Queries "+openQueries+"</p>";
			debug.data(JSON.stringify(query,null,' '));
			text = "<html>"+tableCSSStyle+"<body>"+beforeText+"<table border=\"1\">\n" + tableheader + tablebody + "</table>"+pagefooter+"</body></html>";
			res.set('Content-Type', 'text/html');
			res.end(text);
		};
	})

}


