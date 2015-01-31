var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');
  debug.data = require('debug')('display:data');
  debug.entry = require('debug')('display:entry');
  
var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;
var util     = require('./util.js');
var htmlPage     = require('./htmlPage.js');

var wochenaufgabe = require('./wochenaufgabe.js');






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


exports.main = function(req,res){
	page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname, "html","index.html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));
	page.footer = "OSM Count...";	
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	page = htmlPage.create();
	
	page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "html",aufgabe+".html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));	
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


function listValuesTable(keyname,key,object) {
	if (key == '_id') return;
	if (typeof(object) == 'object') {
		result = "";
		for (k in object) {
			result += listValuesTable(key+"."+k,k,object[k]);
		}
		return result;
    }
    return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";
    
}

exports.object = function(req,res) {
	debug.entry("exports.object");
	var db = res.db;
   
	var collectionName = req.params["collection"];
	var objid = req.params["id"];
	console.log(objid);
	var object=ObjectID(objid);
   
    db.collection(collectionName).findOne ({_id:object},function handleFindOneObject(err, obj) {
    	debug.entry("handleFindOneObject");
    	if (err) {
    		var text = "Display Object "+ objid + " in Collection "+collectionName;
    		res.set('Content-Type', 'text/html');
    		res.end(err);
    	} else {
    		text = "<tr><th>Key</th><th>Value</th><tr>";
    		text+= listValuesTable("","",obj);
    		
    		page =new htmlPage.create("table");
			page.title = "Data Inspector";
			page.menu ="";
			page.content = '<p><table>'+text+'</table></p>';
 			res.set('Content-Type', 'text/html');
 			res.end(page.generatePage());
    	}
    })
}

function generateQuery(measure,schluessel,sub) {
	debug.entry("generateQuery(%s,%s,%s)",measure,schluessel,sub);
   
	
	var subQuery ="";
	if (sub == "missing.name") subQuery = "name!~'.'";
	if (sub == "missing.wheelchair") subQuery = "wheelchair!~'.'";
	if (sub == "missing.phone") subQuery = "phone!~'.']['contact:phone'!~'.'";
	if (sub == "missing.opening_hours") subQuery = "opening_hours!~'.'";
	if (sub == "existing.fixme") subQuery = "fixme";
	
	var query = wochenaufgabe.map[measure].overpass.query;
	if (sub !='') {
		query = wochenaufgabe.map[measure].overpass.querySub;
	}
	query = query.replace('"=":schluessel:"','"~"^'+schluessel+'"');
	query = query.replace('[date:":timestamp:"]','');
	
	
	
	// This should be better done in a while loop
	query = query.replace("out ids;","out;");
	query = query.replace("out ids;","out;");
	query = query.replace("out ids;","out;");
	
	if (typeof(sub) != 'undefined') {
	
		// This should be better done in a while loop
		query = query.replace(':key:',subQuery);
		query = query.replace(':key:',subQuery);
		query = query.replace(':key:',subQuery);
	}
	return query;
}

function generateQueryCSV(measure,schluessel) {
  debug.entry("generateQueryCSV(%s,%s)",measure,schluessel);
  var query = wochenaufgabe.map[measure].overpass.query;
  query = wochenaufgabe.map[measure].overpass.query;
  fieldList = wochenaufgabe.map[measure].overpass.csvFieldList;
  query = query.replace('[out:json]',fieldList);
  query = query.replace('"=":schluessel:"','"~"^'+schluessel+'"');
  query = query.replace('[date:":timestamp:"]','');	
  query = query.replace("out;","out meta;");
	
  // This should be better done in a while loop
  query = query.replace("out ids;","out meta;");
  query = query.replace("out ids;","out meta;");
  query = query.replace("out ids;","out meta;");
	
  return query;
}


exports.overpass = function(req,res) {
	debug.entry("exports.overpass");
	var db = res.db;
   
	var measure = req.params["measure"];
	var schluessel = req.params["schluessel"];
	
	
	var sub = req.query.sub;
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
	page = htmlPage.create();
	page.content = text;
	
	res.set('Content-Type', 'text/html');		
    res.end(page.generatePage());
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

	// since
	var since = param.since;
	if (newLink.hasOwnProperty("since" )) since = newLink.since;
	link += "&since="+since;

	
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
    param.measure="";    
    if(typeof(req.params["measure"])!='undefined') {
    	param.measure=req.params["measure"];
    }
    param.csv = false;
    param.html = false;
    if(typeof(req.params["type"])!='undefined') {
    	if (req.params["type"] == "csv") param.csv = true;
    	if (req.params["type"] == "html") param.html = true;
    }
    
    param.sort ="";
    if(typeof(req.query["sort"])!='undefined') {
    	param.sort=req.query["sort"];
    	param.sortAscending = 1;
    }
    if(typeof(req.query["sortdown"])!='undefined') {
    	param.sort=req.query["sortdown"];
    	param.sortAscending = -1;
    }
    param.since='';
    if(typeof(req.query["since"])!='undefined') {
    	param.since=req.query["since"];
    }
    param.lengthOfKey = 2;
    if(typeof(req.query["lok"])!="undefined") {
    	param.lengthOfKey=parseInt(req.query["lok"]);
    	if (param.lengthOfKey<0) param.lengthOfKey = 0;
    	if (param.lengthOfKey > 12) param.lengthOfKey = 12;
    }
    
    param.lengthOfTime=7;
    param.period="Monat";
    if(req.query["period"]=="Jahr") {
    	param.lengthOfTime=4;
    	param.period="Jahr";
    }   
    if(req.query["period"]=="Monat") {
    	param.lengthOfTime=7;
    	param.period="Monat";
    }
    if(req.query["period"]=="Tag") {
    	param.lengthOfTime=10;
    	param.period="Tag";
    }
   	param.location ="";
    param.locationName="";
    param.locationType="-";
    if(typeof(req.query["location"])!='undefined') {
    	param.location=req.query["location"];
    	param.locationName = param.location;
    	param.locationType="-";
    }
    param.sub = "";
    if (typeof(req.query["sub"]) != 'undefined') {
    	param.sub = req.query["sub"];
    }
    param.subPercent = "";
    if (typeof(req.query["subPercent"]) != 'undefined') {
    	param.subPercent = req.query["subPercent"];
    }
    

}

function generateTable(param,header,firstColumn,table,format,rank, serviceLink) {
	debug.entry("generateTable");
	debug.data(JSON.stringify(rank));
	
	var tableheader = "";
	var tablebody="";
	
	if (typeof(serviceLink)=='undefined') serviceLink = true;
	
	var sumrow = [];
	
	
	
	for (i=0;i<header.length;i++) {
		var cell = header[i];
		var celltext = cell;
		if (cell == param.sort) {
			celltext = "#"+cell+"#";
		}
		if (format[cell] && typeof(format[cell].headerLink) != 'undefined') {
			celltext = '<a href="'+format[cell].headerLink+'">' + celltext + '</a>';
		}
		if (format[cell] && typeof(format[cell].toolTip) != "undefined") {
			celltext = '<p title="'+ format[header[i]].toolTip+ '">'+celltext+'</p>';
		}
		tableheader +="<th class=header>"+celltext+"</th>";
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
			tablerow += "<td><a href=/overpass/"+param.measure+"/"+row+".html"+sub+">O</a>";
			query= generateQuery(param.measure,row,param.sub);
			tablerow += " <a href=http://overpass-turbo.eu/?Q="+encodeURIComponent(query)+"&R>R</a>"
			query= generateQueryCSV(param.measure,row);
			tablerow += " <a href=http://overpass-turbo.eu/?Q="+encodeURIComponent(query)+">#</a></td>"
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
				cell = numeral(content).format('0,0.0'); 
			} else {
				cell = '"'+content+'"';
			}
			if (z>0) tablerow += delimiter;
			tablerow += cell;
		}
		tablerow = tablerow+"\n";
		tablebody += tablerow;
	}
	return tableheader + tablebody;
}

function optionValue(value,displayValue,selected) {
	if (selected == value) {
		return '<option value ="'+value+'" selected><b>'+displayValue+'</b></option>';
	} else  {
		return '<option value ="'+value+'">'+displayValue+'</option>';
	}
}

function generateFilterTable(param,header) {
	debug.entry("generateFilterTable");

    filterSub = "-"
    filterSubPercent = "-";
    subSelector = '';
    subPercentSelector = "";
    if (param.measure == "Apotheke") { 
    	filterSub =  gl("[Name]", {sub:"missing.name"},param);
    	filterSub += gl("[Öffnungszeiten]", {sub:"missing.opening_hours"},param);
    	filterSub += gl("[fixme]", {sub:"existing.fixme"},param);
    	filterSub += gl("[phone]", {sub:"missing.phone"},param);
    	filterSub += gl("[wheelchair]", {sub:"missing.wheelchair"},param);
    	filterSub += gl("[ALLES]", {sub:""},param);
    	
    	subSelector += optionValue("missing.name","Name",param.sub);
    	subSelector += optionValue("missing.opening_hours","Öffnungszeiten",param.sub);
    	subSelector += optionValue("existing.fixme","fixme",param.sub);
    	subSelector += optionValue("missing.phone","Telefon",param.sub);
    	subSelector += optionValue("missing.wheelchair","wheelchair",param.sub);
    	subSelector += optionValue("","Alle Apotheken",param.sub);
    	
    	subSelector = '<select name ="sub">'+subSelector+'</select>';
    	
    	filterSubPercent =  gl("[Prozentanzeige]", {subPercent:"Yes"},param);
    	filterSubPercent += gl("[Anzahl]", {subPercent:"No"},param);
    	subPercentSelector = "";
    	if (param.subPercent == "Yes") {
    		subPercentSelector = '<select name="subPercent"> \
				<option value="Yes" selected>Prozentanzeige</option> \
				<option value="No">Anzahl</option> </select>'
		} else {
    		subPercentSelector = '<select name="subPercent"> \
				<option value="Yes">Prozentanzeige</option> \
				<option value="No" selected>Anzahl</option> </select>'
		} 
    }
    
    
    
    lokSwitch ="1";   
    lokShow = "X";
    for (i =1;i<param.lengthOfKey;i++) {
    	lokShow += "X";
    	lokSwitch += gl("<b> "+(i+1)+"</b>",{lok:(i+1)},param);
    	
    }
    lokSelector = '<select name="lok"> ';
    
    for (;i<10;i++) {
    	lokShow += "-";
    	lokSwitch += gl(" "+(i+1)+"",{lok:(i+1)},param);
    }
    for (;i<12;i++) {
    	lokSwitch += gl(" "+(i+1)+" ",{lok:(i+1)},param);
    }
    for (i=2;i<12;i++) {
   		if (i == param.lengthOfKey) {
    		lokSelector += '<option value="'+i+'" selected>'+i+'</option>';
    	} else {
    		lokSelector += '<option value="'+i+'">'+i+'</option>';
    	}
    }

    
	filterSelector = "";
    var filterText = "";
    if (param.location!="") {
    	var kreisnamen =  wochenaufgabe.map[param.measure].keyMap; 
    	filterText+= param.locationName+" ("+param.location+")";
    	filterSelector += optionValue(param.location,filterText,param.location);
    	for (i=param.location.length-1;i>=1;i--) {
    		location = param.location.substr(0,i);
    		if (typeof(kreisnamen[location])=='undefined') {
    			continue;
    		}
    		n = kreisnamen[location].name;
    		ft = n+" ("+location+")";
    		filterSelector += optionValue(location,ft,param.location);		 
    	}
    	
    } else {
    	filterText +="kein Filter";
    }
    filterSelector += optionValue("","Alle Orte",param.location);
    filterSelector = '<select name ="location">'+filterSelector+'</select>';

    

    var periodenSwitch = gl("[Jahr]",{period:"Jahr"},param);
    periodenSwitch+= gl("[Monat]",{period:"Monat"},param);
    periodenSwitch+= gl("[Tag]",{period:"Tag"},param);
    
    periodenSelector = '<select name="period"> \
			<option value="Jahr"' +((param.period == "Jahr") ? " selected":"")+ '>Jahr</option> \
			<option value="Monat"' +((param.period == "Monat") ? " selected":"")+ '>Monat</option> \
			<option value="Tag"' +((param.period == "Tag") ? " selected":"")+ '>Tag</option> \
			</select>';
	date = new Date();
	date7 = new Date();
	date10 = new Date();
	date14 = new Date();
	
	date2 = date.getDate();
	date7.setDate(date2-7);
	date10.setDate(date2-10);
	date14.setDate(date2-14);
	
	since = '';
	since7 = date7.toISOString().substr(0,10);
	since10 = date10.toISOString().substr(0,10);
	sincex = date14.toISOString().substr(0,10);
	
	if ((param.since != since7) && (param.since != since) && (param.since!= since10)&& (param.since!= sincex)) {
		sincex = param.since;
	}
	
	
    sinceSelector = '<select name="since"> \
			<option value="'+since+'"' +((param.since == since) ? " selected":"")+ '>'+since+'</option> \
			<option value="'+since7+'"' +((param.since == since7) ? " selected":"")+ '>'+since7+'</option> \
			<option value="'+since10+'"' +((param.since == since10) ? " selected":"")+ '>'+since10+'</option> \
			<option value="'+sincex+'"' +((param.since == sincex) ? " selected":"")+ '>'+sincex+'</option> \
			</select>';
    
	
	var filterTableL1, filterTableL2;
	
	// Filter on Location
	filterTableLH = '<th class = "menu" > Ort </th>';
	filterTableL1 = '<td class = "menu" >'+filterText+'</td>';
	filterTableL2 = '<td class = "menu">'+filterSelector+'</td>';
	// Filter on Key
	filterTableLH += '<th class = "menu"> Anzahl / Tags</th>';
	filterTableL1 += '<td class = "menu">'+param.sub+'</td>';
	filterTableL2 += '<td class = "menu">'+subSelector+'</td>';
	// Filter on Percent
	filterTableLH += '<th class = "menu"> Anzahl / % Angabe</th>';
	filterTableL1 += '<td class = "menu">'+((param.subPercent!="Yes")?'Anzahl':'%')+'</td>';
	filterTableL2 += '<td class = "menu">'+subPercentSelector+'</td>';
	// Filter on Period
	filterTableLH += '<th class = "menu">Zeitachse</th>';
	filterTableL1 += '<td class = "menu">'+param.period+'</td>';
	filterTableL2 += '<td class = "menu">'+periodenSelector+'</td>';
	// Filter on since
	filterTableLH += '<th class = "menu">Seit</th>';
	filterTableL1 += '<td class = "menu">'+param.since+'</td>';
	filterTableL2 += '<td class = "menu">'+sinceSelector+'</td>';
	// Filter on length Of Key
	filterTableLH += '<th class = "menu"> Schlüssellänge</th>';
	filterTableL1 += '<td class = "menu">'+param.lengthOfKey+'</td>';
	filterTableL2 += '<td class = "menu">'+lokSelector+'</td>';
	// Aktion
	filterTableLH += '<th class = "menu"></th>';
	filterTableL1 += '<td class = "menu">'+''+'</td>';
	filterTableL2 += '<td class = "menu">'+'<input type="submit" value="Parameter Umstellen">'+'</td>';
	// Plotly Integration
	filterTableLH += '<th class = "menu">Graphen</th>';
	filterTableL1 += '<td class = "menu">'+'<a href="/waplot/'+param.measure+'.html?location='+param.location+'&lok='+param.lengthOfKey+'">Zeige Anzahl als Grafik</a>'+'</td>';
	filterTableL2 += '<td class = "menu">'+'<a href="/wavplot/'+param.measure+'.html?location='+param.location+'">Zeige Tags als Grafik</a>'+'</td>';
	// CSV Export	
	filterTableLH += '<th class = "menu">sonstiges</th>';
	filterTableL1 += '<td class = "menu">'+gl("Als CSV Downloaden",{csv:true},param)+'</td>';
	filterTableL2 += '<td class = "menu">'+'<a href="/wa/'+param.measure+'.html">Hilfe / Informationen</a>'+'</td>';
	
	
	
	filterTable = '<table class="menu"><tr>'+filterTableLH+'</tr> \
										<tr>'+filterTableL1+'</tr> \
										<tr>'+filterTableL2+'</tr></table>';
	
	
	//filterTable = "<tr><td>"+filterText+
	//filterTable = "<b>Gefiltert Auf:"+filterText + "</b> "+ filterSelector+ subSelector + subPercentSelector + periodenSelector + lokSelector + '<input type="submit" value="Parameter Umstellen">';
	filterTable = "<form>"+filterTable+"</form>";
	
	return filterTable;

}

function generateSortHeader(param,header,format) {
	debug.entry("generateSortHeader");


    
    for (i=0;i<header.length;i++) {
    	if (header[i]=="Admin Level") continue;
    	if (header[i]=="Name") continue;
    	link = "";
    	if ((param.sortAscending == 1) && (header[i] == param.sort)) {
    		link = gl("", {sortdown:header[i]},param)
    	} else {
    		link = gl("", {sort:header[i]},param)
    	}
    	if (!format[header[i]]) format[header[i]] = {};
    	format[header[i]].headerLink = link;
    	
    }
}

exports.table = function(req,res){
	debug.entry("exports.table");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    var collectionTarget = db.collection('DataTarget');
    

	
	numeral = util.numeral;

	param = {};
    setParams(req,param);
    
    
    if (typeof(wochenaufgabe.map[param.measure])=='undefined') 
    {
    	res.set('Content-Type', 'text/html');
		res.end("Die Wochenaufgabe "+param.measure+ " ist nicht definiert.");
    	return;
    }
    
 	var kreisnamen =  wochenaufgabe.map[param.measure].keyMap; 
   

    
    v = kreisnamen[param.location];
	if (typeof(v)!='undefined') {
		param.locationName=v.name;
		param.locationType=v.typ;
	}
    
    
   
    

    
    ranktype =wochenaufgabe.map[param.measure].ranktype;

   
 
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
    date = new Date();
    date.setDate(date.getDate()-10*365);
    if (param.since != '') date = new Date(param.since);
    var filterOneMeasure = {$match: { measure: param.measure}};
    var filterSince = {$match: { timestamp: {$gte: date}}};
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
    			 filterSince,
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
 	var errorQueries=0;
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
			},
			function getErrorQueueCount(callback) {
				debug.entry("getErrorQueueCount");
				db = res.db;
				collectionName = 'WorkerQueue';
				
				// Fetch the collection test
				var collection = db.collection(collectionName);
				date = new Date();
				collection.count({status:"error",exectime: 
								 {$lte: date},measure:param.measure},
								 function getWorkerQueueCountCB(err, count) {
					debug.entry("getWorkerQueueCount");
					errorQueries=count;
					callback();
				});  
			}
			],
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
				format["Schlüssel"].toolTip = wochenaufgabe.map[param.measure].key;
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
					page =new htmlPage.create("table");
					page.title = "Wochenaufgabe "+param.measure;
					page.menu = generateFilterTable(param,header);
					
					generateSortHeader(param,header,format);
					var table = generateTable(param,header,firstColumn,table,format, rank);
					
					page.content = '<p><table>'+table+'</table></p>';
					
					pageFooter = "";
					if (openQueries > 0) {
						pageFooter += "<b>Offene Queries "+openQueries+".</b> ";
					}
					if (errorQueries > 0) {
						pageFooter += "<b>Fehlerhafte Queries "+errorQueries+".</b> ";
					}
					pageFooter += "Die Service Links bedeuten: \
									<b>O</b> Zeige die Overpass Query \
									<b>R</b> Starte die Overpass Query \
									<b>#</b> Öffne Overpass Turbo mit CSV Abfrage"
					page.footer = pageFooter;
						
					debug.data(JSON.stringify(query,null,' '));
					
					res.set('Content-Type', 'text/html');
					res.end(page.generatePage());
					return;
				} 
				if (param.csv) {
					var table = generateCSVTable(param,header,firstColumn,table,";");
					if (openQueries > 0 ) {
						table = "Achtung: Es laufen noch "+openQueries+" Overpass Abfragen, das Ergebnis ist eventuell unvollständig\n"+table;
					}
					res.set('Content-Type', 'application/octet-stream');
					// var bom = String.fromCharCode(239 ); // Hex:EF BB BF, Dec: 239 187 191
					// bom += String.fromCharCode( 187); 
					// bom += String.fromCharCode( 191); 
					// var bom = '\xEF\xBB\xBF'
					res.end(table);
					return;
				}
				res.set('Content-Type', 'text/html');
				res.end("<body> Unbekannter Typ <body>");
				
			}
		)

}

function getValue(columns,object,d) {
   var result;
   if (typeof(columns) == 'object') {
   	if (typeof(object[columns[d]])=='object') {
   		return getValue(columns,object[columns[d]],d+1);
   	} else {
   	
   		result = object[columns[d]];
   	}
   } else {
   	result = object[colums];
   }
   return (typeof(result)=='undefined')?"":result;
}
exports.query=function(req,res) {
	debug.entry("exports.query");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection;
    
    switch (req.params.query) {
    	case "WorkerQueue": collection = db.collection('WorkerQueue');
    						collectionName = "WorkerQueue";
    	               columns = ["_id","type","status","measure"];
    	               query = {type : "insert"};
    	               break;
    	case "pharmacy": collection = db.collection('POI');
    	                 collectionName = "POI";
    	                  columns = ["_id",
    	                            ["name","tags","name"],
    	                            ["PLZ","nominatim","postcode"],
    	                             ["Ort","nominatim","town"],
    	                             ["Straße","nominatim","road"],
    	                             ["Hausnummer","nominatim","house_number"],
    	                             ["Telefon","tags","phone"]
    	                             ];
    	                  query = {};
    	                  break;
    	 default:collection = db.collection('WorkerQueue');
    	               columns = ["_id","type","status","measure"];
    }
    
    collection.find(query).toArray(function(err,data) {
    	if (err) {
    			res.set('Content-Type', 'text/html');
				res.end(JSON.stringify(err));
    	}
    	console.log(err);
    	table = "";
    	tablerow = "";
    	for (j=0;j<columns.length;j++) {
    		header = columns[j];
    		if (typeof(columns[j])=='object') {
    			header = columns[j][0];
    		}
    		tablerow += "<th>"+header+"</th>";
    	}
    	tablerow = "<tr>"+tablerow+"</tr>";
    	table += tablerow;
    	console.log(data.length);
    	for (i = 0;i<data.length;i++) {
    	  tablerow = "";
    	  d = data [i];
    	  
    	  tablerow = '<td> <a href="/object/'+collectionName+'/'+d._id+'.html">'+d._id+'</a></td>';
    	  key = columns[j];
    	  for (j=1;j<columns.length;j++) {
      		tablerow += "<td>"+getValue(columns[j],d,1);+"</td>";
    	  }
    	  tablerow = "<tr>"+tablerow+"</tr>";
    	  table += tablerow;
    	}
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.menu ="";
		page.content = '<p><table>'+table+'</table></p>';
		pageFooter = "";
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
   
    })
    
    
}
    
    

	

