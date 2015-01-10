var loadDataFromDB = require('./LoadDataFromDB')
var loadOverpassData = require('./LoadOverpassData')
var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');
  debug.data = require('debug')('display:data');
  debug.entry = require('debug')('display:entry');
  
var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;
var util     = require('./util.js');



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
td.first { \
background-color: #00ff00; \
} \
td.second { \
background-color: #88FF88; \
} \
td.last { \
background-color: #ff0000; \
} \
td.lastButOne { \
background-color: #ff8888; \
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
    collection.count(function handleCollectionCount(err, count) {
    	debug.entry("handleCollectionCount");
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
   
    db.collection(collectionName).findOne ({_id:object},function handleFindOneObject(err, obj) {
    	debug.entry("handleFindOneObject");
    	if (err) {
    		var text = "Display Object "+ objid + " in Collection "+collectionName;
    		res.set('Content-Type', 'text/html');
    		res.end(err);
    	} else {
    		text = "";
    		for (key in obj) {
    			if (key != "_id") {
    				text += "<tr><td>"+key+"</td><td>"+obj[key]+"</td></tr>";
    			}
    		}	
    		text = tableCSSStyle+"<body><h1>Data Inspector</h1><table>"+text+"</table></body>";
    		res.set('Content-Type', 'text/html');
    		res.end(text );
    	}
    })
}

function generateQuery(measure,schluessel,sub) {
	debug.entry("generateQuery");
   
	
	var subQuery ="";
	if (sub == "missing.name") subQuery = "name!~'.'";
	if (sub == "missing.wheelchair") subQuery = "wheelchair!~'.'";
	if (sub == "missing.phone") subQuery = "phone!~'.']['contact:phone'!~'.'";
	if (sub == "missing.opening_hours") subQuery = "opening_hours!~'.'";
	if (sub == "existing.fixme") subQuery = "fixme";
	
	
	
	if ( sub != '' && measure == "Apotheke") {
		measure = "ApothekeDetail";
	}
	
	var query = loadOverpassData.query[measure];
	query = query.replace('"="######"','"~"^'+schluessel+'"');
	
	// This should be better done in a while loop
	query = query.replace("out ids;","out;");
	query = query.replace("out ids;","out;");
	query = query.replace("out ids;","out;");
	
	if (typeof(sub) != 'undefined') {
	
		// This should be better done in a while loop
		query = query.replace('$$$$',subQuery);
		query = query.replace('$$$$',subQuery);
		query = query.replace('$$$$',subQuery);
	}
	return query;
}


exports.overpass = function(req,res) {
	debug.entry("exports.overpass");
	var db = res.db;
   
	var measure = req.param("measure");
	var schluessel = req.param("schluessel");
	
	
	var sub = req.param("sub");
	if (typeof(sub) == 'undefined') sub = "";
    query = generateQuery(measure,schluessel,sub);
    
	if (!query) query = "Für die Aufgabe "+measure+" ist keine Query definiert";

	var text = "<h1>Overpass Abfrage</h1>"
	text += "<table><tr><td>Aufgabe</td><td>"+measure+"</td></tr> \
						<tr><td>Schl&uuml;ssel</td><td>"+schluessel+"</td></tr></table>";
	
	text += "<pre>"+query+"</pre>"
	
	text += '<p>Bitte die Query in die Zwischenablage kopieren und in <a href=http://overpass-turbo.eu>Overpass Turbo</a> einf&uuml;gen</p>';
	
	text += '<p> Oder <a href=http://overpass-turbo.eu/?Q='+encodeURIComponent(query)+'&R>hier</a> klicken';
	if (measure=="AddrWOStreet") {
		text += '<p>Achtung, die Overpass Abfrage und die Abfrage von User:Gehrke unterschieden sich etwas. Siehe <a href="http://wiki.openstreetmap.org/wiki/DE:Overpass_API/Beispielsammlung#Hausnummern_ohne_Stra.C3.9Fe_finden">wiki</a>.</p>';
	}
	text = "<html>"+tableCSSStyle+"<body>"+text+"</body></html>";
    res.set('Content-Type', 'text/html');		
    res.end(text);
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
    res.set('Content-Type', 'text/html');
    res.end(text);
}
	
exports.importApotheken = function(req,res) {
	debug.entry("importApotheken");
    var db = res.db;
  
    importCSV.importApothekenVorgabe(db);
    text = "Importiert Apotheken";
    text += "Files imported";
    res.set('Content-Type', 'text/html');
    res.end(text);
}
	
function gl(text, newLink, param)
{
	debug.entry("gl");
	var link = "/table";
	var type = "html";
	if (newLink.csv == true) type = "csv";

	// measure
	var measure = param.measure;
	if (newLink.hasOwnProperty("measure" )) measure = newLink.measure;

	// Reset Params when switching measure
	if (measure != param.measure) {
		newLink.sub = "";
	}
	link += "/"+measure+"."+type;
	// Missing Tags
	var sub = param.sub;
	if (newLink.hasOwnProperty("sub" )) sub = newLink.sub;
	link += "?sub="+sub;	
	
	// location
	
	var location = param.location;
	if (newLink.hasOwnProperty("location")) location = newLink.location;
	link += "&location="+location;

	// lengthOfKey
	var lok = param.lengthOfKey;
	if (newLink.hasOwnProperty("lok" )) lok = newLink.lok;
	link += "&lok="+lok;

	// period
	var period = param.period;
	if (newLink.hasOwnProperty("period" )) period = newLink.period;
	link += "&period="+period;

	
	// sortierung
	var sort = param.sort;
	var ascending = 1;
	if (newLink.hasOwnProperty("sort")) sort = newLink.sort;
	if (newLink.hasOwnProperty("sortdown")) {
		sort = newLink.sortdown;
		ascending = -1;
	}
	
	
	link += ((ascending==1) ? "&sort=": "&sortdown=")+encodeURIComponent(sort);


	// subPercent
	var subPercent = param.subPercent;
	if (newLink.hasOwnProperty("subPercent")) subPercent = newLink.subPercent;
	
	link += "&subPercent="+subPercent;
	
	if (text == "") {
		return link;
	} else {
		return "<a href="+link+">"+text + "</a>";
	}
}

function setParams(req,param) {
	debug.entry("setParams");
    // Parse html parameters
    param.measure="Apotheke";    
    if(typeof(req.param("measure"))!='undefined') {
    	param.measure=req.param("measure");
    }
    param.csv = false;
    param.html = false;
    if(typeof(req.param("type"))!='undefined') {
    	if (req.param("type") == "csv") param.csv = true;
    	if (req.param("type") == "html") param.html = true;
    }
    
    param.sort ="";
    if(typeof(req.param("sort"))!='undefined') {
    	param.sort=req.param("sort");
    	param.sortAscending = 1;
    }
    if(typeof(req.param("sortdown"))!='undefined') {
    	param.sort=req.param("sortdown");
    	param.sortAscending = -1;
    }
    param.lengthOfKey = 2;
    if(typeof(req.param("lok"))!="undefined") {
    	param.lengthOfKey=parseInt(req.param("lok"));
    	if (param.lengthOfKey<0) param.lengthOfKey = 0;
    	if (param.lengthOfKey > 12) param.lengthOfKey = 12;
    }
    
    param.lengthOfTime=7;
    param.period="Monat";
    if(req.param("period")=="Jahr") {
    	param.lengthOfTime=4;
    	param.period="Jahr";
    }   
    if(req.param("period")==" Monat") {
    	param.lengthOfTime=7;
    	param.period="Monat";
    }
    if(req.param("period")=="Tag") {
    	param.lengthOfTime=10;
    	param.period="Tag";
    }
   	param.location ="";
    param.locationName="";
    param.locationType="-";
    if(typeof(req.param("location"))!='undefined') {
    	param.location=req.param("location");
    	param.locationName = param.location;
    	param.locationType="-";
    }
    param.sub = "";
    if (typeof(req.param("sub")) != 'undefined') {
    	param.sub = req.param("sub");
    }
    param.subPercent = "";
    if (typeof(req.param("subPercent")) != 'undefined') {
    	param.subPercent = req.param("subPercent");
    }
    

}

function generateTable(param,header,firstColumn,table,format,rank, serviceLink) {
	debug.entry("generateTable");
	debug.data(JSON.stringify(rank));
	
	var tableheader = "";
	var tablebody="";
	
	if (!serviceLink) serviceLink = true;
	
	var sumrow = [];
	
	
	
	for (i=0;i<header.length;i++) {
		var cell = header[i];
		if (cell == param.sort) {
			cell = "&#8691 <i>"+cell+"</i> &#8691";
		}
		if (format[cell] && typeof(format[cell].toolTip) != "undefined") {
			cell = '<p title="'+ format[header[i]].toolTip+ '">'+cell+'</p>';
		}
		tableheader +="<th>"+cell+"</th>";
	}
	if (serviceLink) tableheader += "<th> Service </th>";
	tableheader = "<tr>"+tableheader + "</tr>";
	
	for (i=0;i<firstColumn.length;i++) {
		row = firstColumn[i];
		
		for (z=0;z<header.length;z++) {
			col=header[z];
			
			var content=table[row][col];
			
			var func = (format[col]) ? format[col].func: null;
		
			if (func && typeof(func) != 'undefined') {
				switch (func.op) {
					case "%": content = table[row][func.numerator]/table[row][func.denominator];
					   break;
			 		case "-": content = table[row][func.op1]-table[row][func.op2];
			  	   		break;
				  default: content = "NaF";
				}
				table[row][col] = content;
			}
		}
	}
	var first = [];
	var second = [] ;
	var last=[]  ;
	var lastButOne = [];
	for (c=0;c<header.length;c++) {
		col = header[c];
		if (typeof(rank[col])!='undefined' && table[firstColumn[0]]) {
			value = table[firstColumn[0]][col];
			first[col] = second[col] = last[col] = lastButOne[col] = value;
			for (i=1;i<firstColumn.length;i++)
			{
				value = table[firstColumn[i]][col];
				if (value>first[col]) {
					second[col]=first[col];
					first[col] = value; 
				}
				if ((value > second[col]) && (value < first[col]))
				{
					second[col] = value;
				}
				if (value<last[col]) {
					lastButOne[col]=last[col];
					last[col] = value; 
				}  
				if ((value < lastButOne[col]) && (value > last[col]))
				{
					lastButOne[col] = value;
				}
			}
		}
	}

	// Sort the table by the parameter
	if (header.indexOf(param.sort>=0)) {
		firstColumn.sort( function(a,b) {
			va = table[a][param.sort];
			vb = table[b][param.sort];
			return (vb-va)*param.sortAscending});
	}
	for (i=0;i<firstColumn.length;i++)
	{
		row = firstColumn[i];
		tablerow = "";
		line = table[row];
		
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
				if (format[col] && format[col].sum) {
					if (typeof (sumrow[col]) == 'undefined') {
						sumrow[col]=0;
					}
					sumrow[col]+=content;
				}
			}
			if (glink) {
				cell = '<a href="'+glink(cell)+'">'+cell+'</a>';
			}
			var cl ="";
			if ((typeof(rank[col])!="undefined")  && (rank[col]=="up") ){
			
				if (content == lastButOne[col]) cl = 'class = "lastButOne"';
				if (content == last[col]) cl = 'class = "last"';
				if (content == second[col]) cl = 'class = "second"';
				if (content == first[col]) cl = 'class = "first"';
			
			}
			if ((typeof(rank[col])!="undefined")  && (rank[col]=="down") ) {
			
				if (content == lastButOne[col]) cl = 'class = "second"';
				if (content == last[col]) cl = 'class = "first"';
				if (content == second[col]) cl = 'class = "lastButOne"';
				if (content == first[col]) cl = 'class = "last"';
			
			}
			tablerow += "<td "+cl+">"+cell+"</td>";
		}
		if (serviceLink) {
			var sub = "";
			if (param.sub != "") sub = "?sub="+param.sub;
			tablerow += "<td><a href=./overpass/"+param.measure+"/"+row+".html"+sub+">O</a>";
			query= generateQuery(param.measure,row,param.sub);
			tablerow += " <a href=http://overpass-turbo.eu/?Q="+encodeURIComponent(query)+"&R>R</a></td>"
		}
		tablerow = "<tr>"+tablerow+"</tr>";
		tablebody += tablerow;
	}
	tablerow = "";
	line = sumrow;
	for (z=0;z<header.length;z++) {
		col=header[z];
	
		var cell = "";
		if (z==0) cell = "Summe";
		var content=sumrow[col];
		var f = (format[col]) ? format[col].format: null;
		glink = (format[col]) ? format[col].generateLink:null;
		
		var func = (format[col]) ? format[col].func: null;
		
		if (func && typeof(func) != 'undefined') {
			switch (func.op) {
			  case "%": content = sumrow[func.numerator]/sumrow[func.denominator];
			  	   break;
			  case "-": content = sumrow[func.op1]-sumrow[func.op2];
			  	   break;
			  default: content = "NaF";
			}
		}
		if (typeof(content) != "undefined" ) {
			if (f) {
				cell = numeral(content).format(f);
			} else if (typeof(content)=='number') {
				cell = numeral(content).format();
			} else {
				cell = content; // numeral(content).format();
			}
		}
	
		tablerow += "<td><b>"+cell+"</b></td>";
	}	
	tablerow = "<tr>"+tablerow+"</tr>";
	tablebody += tablerow;
	return tableheader + tablebody;
}

function generateCSVTable(param,header,firstColumn,table,delimiter) {
	debug.entry("generateCSVTable");
	
	var tableheader = "";
	var tablebody="";
	
	
	for (i=0;i<header.length;i++) {
		var cell = header[i];
		if (i>0) tableheader += delimiter;
		tableheader += cell;
	}
	tableheader += "\n";
	

	for (i=0;i<firstColumn.length;i++)
	{
		row = firstColumn[i];
		tablerow = "";
		line = table[row];
		
		for (z=0;z<header.length;z++) {
			col=header[z];
			
			var cell;
			var content=table[row][col];
			
			if (typeof(content) == "undefined") {
				cell ="-";
			} else if (typeof(content) == 'number') {
				cell = numeral(content).format('0,0'); 
			} else {
				cell = content;
			}
			if (z>0) tablerow += delimiter;
			tablerow += cell;
		}
		tablerow = tablerow+"\n";
		tablebody += tablerow;
	}
	return tableheader + tablebody;
}

function generateFilterTable(param,header) {
	debug.entry("generateFilterTable");

    filterSub = "-"
    filterSubPercent = "-";
    if (param.measure == "Apotheke") { 
    	filterSub =  gl("[Name]", {sub:"missing.name"},param);
    	filterSub += gl("[Öffnungszeiten]", {sub:"missing.opening_hours"},param);
    	filterSub += gl("[fixme]", {sub:"existing.fixme"},param);
    	filterSub += gl("[phone]", {sub:"missing.phone"},param);
    	filterSub += gl("[wheelchair]", {sub:"missing.wheelchair"},param);
    	filterSub += gl("[ALLES]", {sub:""},param);
    	filterSubPercent =  gl("[Prozentanzeige]", {subPercent:"Yes"},param);
    	filterSubPercent += gl("[Anzahl]", {subPercent:"No"},param);
    }
    
    
    filterSwitch =   gl("[AddrWOStreet]",{lok:2,period:"Monat",measure:"AddrWOStreet",sub:""},param);
    filterSwitch +=  gl("[Apotheke]",{lok:2,period:"Monat",measure:"Apotheke",sub:""},param);
    
    lokSwitch ="1";   
    lokShow = "X";
    for (i =1;i<param.lengthOfKey;i++) {
    	lokShow += "X";
    	lokSwitch += gl("<b> "+(i+1)+"</b>",{lok:(i+1)},param);
    	
    }
    for (;i<10;i++) {
    	lokShow += "-";
    	lokSwitch += gl(" "+(i+1)+"",{lok:(i+1)},param);
    }
    for (;i<12;i++) {
    	lokSwitch += gl(" "+(i+1)+" ",{lok:(i+1)},param);
    }
    
    //if (param.lengthOfKey >2) lokSwitch += generateLink("weniger",basisLink,"lok="+(param.lengthOfKey-1),paramTime,paramMeasure,paramLocation)+" ";
    //if (param.lengthOfKey <12) lokSwitch+= generateLink("mehr",basisLink,"lok="+(param.lengthOfKey+1),paramTime,paramMeasure,paramLocation);

    var filterText = "";
    if (param.location!="") {
    	filterText+= param.locationName+" ("+param.location+","+param.locationType+")";
    	
    } else {
    	filterText +="kein Filter";
    }

    var periodenSwitch = gl("[Jahr]",{period:"Jahr"},param);
    periodenSwitch+= gl("[Monat]",{period:"Monat"},param);
    periodenSwitch+= gl("[Tag]",{period:"Tag"},param);
    
    var sortSwitch = "";
    for (i=0;i<header.length;i++) {
    	if (header[i]=="Admin Level") continue;
    	if (header[i]=="Name") continue;
    	sortSwitch += gl("["+header[i]+"-]", {sortdown:header[i]},param);
    }
    sortSwitch+="<br>";
    for (i=0;i<header.length;i++) {
    	if (header[i]=="Admin Level") continue;
    	if (header[i]=="Name") continue;
    	sortSwitch += gl("["+header[i]+"+]", {sort:header[i]},param);
    }

    var filterTable = "<tr><td>Messung</td><td><b><a href=/"+param.measure+".html>"+param.measure+"</a></b></td><td>"+filterSwitch+"</td></tr>";
  	filterTable += "<tr><td>Tag</td><td><b>"+param.sub+"</b></td><td>"+filterSub+"</td></tr>"  
  	filterTable += "<tr><td>Anzeige %</td><td><b>"+param.subPercent+"</b></td><td>"+filterSubPercent+"</td></tr>"  
	   
    filterTable += "<tr><td>Filter</td><td><b>"+filterText+"</b></td><td>"+gl("[Bundesländer]",{lok:2,location:""},param)+"</td></tr>"
    filterTable += "<tr><td>Periode</td><td><b>"+param.period+"</B></td><td>"+periodenSwitch+"</td></tr>"
    filterTable += "<tr><td>Schlüssellänge</td><td><b>"+lokShow+"</b></td><td>"+lokSwitch+"</td></tr>"
    filterTable += "<tr><td>Sortierung</td><td><b>"+param.sort+"("+param.sortAscending+")"+"</b></td><td>"+sortSwitch+"</td></tr>"
    
	filterTable = "<table>"+filterTable+"</table><br><br>";
	return filterTable;

}


exports.table = function(req,res){
	debug.entry("exports.table");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    var collectionTarget = db.collection('DataTarget');
    

	var kreisnamen = {}; 
	
	numeral = util.numeral;

	param = {};
    setParams(req,param);
    
    if (param.measure != "Apotheke" && param.measure != "AddrWOStreet") 
    {
    	res.set('Content-Type', 'text/html');
		res.end("Die Wochenaufgabe "+param.measure+ " ist nicht definiert.");
    	return;
    }
    
    if (param.measure == "Apotheke") {
    		kreisnamen = loadDataFromDB.schluesselMapAGS;
    }
    if (param.measure == "AddrWOStreet") {
    		kreisnamen = loadDataFromDB.schluesselMapRegio;
    }
    
    v = kreisnamen[param.location];
	if (typeof(v)!='undefined') {
		param.locationName=v.name;
		param.locationType=v.typ;
	}
    
    
   
    var paramTime = "period="+param.period;
    var paramMeasure = "measure="+param.measure;
    var paramLength = "lok="+param.lengthOfKey;
    var paramLocation = "location="+param.location;
    
    beforeText= "<h1> OSM Count </h1>";
    
    ranktype = "";
    if (param.measure == "Apotheke") {
    	ranktype = "UP";
    }
    if (param.measure == "AddrWOStreet") {
    	ranktype = "down";
    }

   
 
    valueToCount = "$count";
   	if (param.sub != "") {
   		valueToCount = "$"+param.sub;
   		ranktype = "down";
   	}
   	
   	valueToDisplay = "$count";
    
    if (param.subPercent == "Yes") {
    	valueToDisplay = { $cond : [ {$eq : ["$count",0]},0,{$divide: [ "$count","$total"]}]};
    	ranktype = "up";
    } 
    
    var filterOneMeasure = {$match: { measure: param.measure}};
    var filterRegionalschluessel = {$match: {schluessel: {$regex: "^"+param.location}}};
 
    var aggregateMeasuresProj = {$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
    						 			  timestamp: "$timestamp",
    						 			  timestampShort: {$substr: ["$timestamp",0,param.lengthOfTime]},
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
			}))},
			function getVorgabe(callback) {
				debug.entry("getVorgabe");
				if (param.measure=="Apotheke" && param.sub =="") {
					collectionTarget.aggregate(	queryVorgabe
									, (function getVorgabeCB(err, data) {
					debug.entry("getVorgabeCB");
					if (err) {
						res.set('Content-Type', 'text/html');
						res.end("error"+err);
						console.log("Table Function, Error occured:");
						console.log(err);
						;
					}
					for (i = 0;i<data.length;i++)
					{
						schluessel = data[i]._id;
						value = data[i].vorgabe;
						vorgabe [schluessel]=value;
					}
					callback();
				}))
			} else callback();
			},
			function getWorkerQueueCount(callback) {
				debug.entry("getWorkerQueueCount");
				db = res.db;
				collectionName = 'WorkerQueue';
				
				// Fetch the collection test
				var collection = db.collection(collectionName);
				date = new Date();
				collection.count({status:"open",exectime: 
								 {$lte: date},measure:param.measure},
								 function getWorkerQueueCountCB(err, count) {
					debug.entry("getWorkerQueueCount");
					openQueries=count;
					callback();
				});  
			}],
			function displayFinalCB (err, results ) {
				debug.entry("displayFinalCB");
				// Initialising JavaScript Map
				var header = [];
				var firstColumn = [];
				var table =[]; 
				var format=[];
				var rank = [];
		
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
						format[col] = {};
						format[col].sum = true;
						rank[col]=ranktype;
						if (param.subPercent == "Yes") {
							format[col] = {};
							format[col].sum = false; 
							format[col].format = '0%'
						}
					}
					if (firstColumn.indexOf(row)<0) {
						firstColumn.push(row);
					}
					if (!Array.isArray(table[row])) {
						table[row]=[];
					}
			
					table[row][col]=parseFloat(measure.cell);
			
				}
				header.sort();

		
				// Extend two dimensional Table
				displayVorgabe = ((param.lengthOfKey > 1)&&(param.measure=="Apotheke") && (param.sub == ""));
		

				if (displayVorgabe) {
					header.unshift("Vorgabe");
					format["Vorgabe"] = {};
					format["Vorgabe"].toolTip = "theoretische Apothekenzahl";
					format["Vorgabe"].sum = true;
			
				}
		
				header.unshift("Admin Level");
				header.unshift("Name");
				format["Name"] = {};
				format["Name"].toolTip = "Name aus OSM, Alternativ Teilschlüssel";
				header.unshift("Schlüssel");
				format["Schlüssel"]= {};
				if (param.measure== "Apotheke") {
					format["Schlüssel"].toolTip = "de:amtlicher_gemeindeschluessel";
				}
				if (param.measure== "AddrWOStreet") {
					format["Schlüssel"].toolTip = "de:regionalschluessel";
				}
				format["Schlüssel"].generateLink = function(value) {
					return gl("",{lok:(param.lengthOfKey+1),location:value},param);
				};
				if (!param.csv) {
					if (displayVorgabe) {
						var colName = "% in OSM"
						rank[colName]="up";
						header.push(colName);
						format[colName]={};
						format[colName].toolTip = "Anzahl Apotheken in OSM / theoretische Apothekenzahl";
						format[colName].format='0%';
						format[colName].sum = false;
						format[colName].func = {};
						format[colName].func.op = "%";
						format[colName].func.denominator  = "Vorgabe";
						format[colName].func.numerator = header[header.length-2];
			
					} else {
						header.push("Diff");
						if (ranktype == "UP" || ranktype == "up") {
							rank["Diff"]="up";
						}
						if (ranktype == "down") {
							rank["Diff"]="down";
						}
						format["Diff"]={};
						format["Diff"].toolTip = "Differenz zwischen "+ header[header.length-2]+ " und " + header[header.length-3];
						format["Diff"].sum = false;
						if (param.subPercent == "Yes") {
							format["Diff"].format ='0%';
						}
						format["Diff"].func = {};
						format["Diff"].func.op = "-";
						format["Diff"].func.op1  = header[header.length-2];
						format["Diff"].func.op2 = header[header.length-3];
					}
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
					table[schluessel]["Schlüssel"]=schluessel;
					table[schluessel]["Name"]=schluesselText;
					table[schluessel]["Admin Level"]=schluesselTyp;
					if (displayVorgabe ){ 
						expectation = vorgabe[schluessel];
						table[schluessel]["Vorgabe"]=expectation;
					}
				}
				
				if (param.html) {
					beforeText += generateFilterTable(param,header);

					var table = generateTable(param,header,firstColumn,table,format, rank);
					pagefooter = "";
					if (openQueries > 0) {
						pagefooter = "<p> Offene Queries "+openQueries+"</p>";
					}
					pagefooter += "<p>"+gl("Als CSV Downloaden",{csv:true},param)+"</p>"
					pagefooter += "<p> Die Service Link(s) bedeuten \
									<li>O Zeige die Overpass Query</li> \
									<li>R Starte die Overpass Query</li> \
									</p>"
					debug.data(JSON.stringify(query,null,' '));
					text = "<html>"+tableCSSStyle+"<body>"+beforeText+"<table border=\"1\">\n" + table + "</table>"+pagefooter+"</body></html>";
					res.set('Content-Type', 'text/html');
					res.end(text);
					return;
				} 
				if (param.csv) {
					var table = generateCSVTable(param,header,firstColumn,table,";");
					res.set('Content-Type', 'application/octet-stream');
					res.end(table);
					return;
				}
				res.set('Content-Type', 'text/html');
				res.end("<body> Unbekannter Typ <body>");
				
			}
		)

}


