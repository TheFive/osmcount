var importCSV=require('./ImportCSV');
var debug    = require('debug')('display');
 
  
var path     = require('path');
var fs       = require("fs");
var async    = require('async');
var ObjectID = require('mongodb').ObjectID;
var util     = require('./util.js');
var htmlPage     = require('./htmlPage.js');

var wochenaufgabe = require('./wochenaufgabe.js');






exports.count = function(req,res){
	debug("exports.count");
    var db = res.db;
    var collectionName = 'DataCollection';
     if(typeof(req.param("collection"))!='undefined') {
     	collectionName = req.param("collection");
     }
  
    // Fetch the collection test
    var collection = db.collection(collectionName);
    collection.count(function handleCollectionCount(err, count) {
    	debug("handleCollectionCount");
    	res.set('Content-Type', 'text/html');
    	if(err) {
    		res.end(err);
    	} else {
    		res.end("There are " + count + " records in Collection "+ collectionName);
    	}
    });  
}


exports.main = function(req,res){
	var page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname, "html","index.html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));
	page.footer = "OSM Count...";	
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	var page = htmlPage.create();
	
	page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "html",aufgabe+".html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));	
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


function listValuesTable(keyname,key,object) {
	if (key == '_id') return "";
	if (object instanceof Date) {
		return "<tr><td>"+keyname+"</td><td>"+object.toString()+"</td></tr>";
	}
	if (object instanceof ObjectID) {
		return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";
	}	
	if (typeof(object) == 'object') {
		var result = "";
		for (k in object) {
			if (key) { 
			  result += listValuesTable(key+"."+k,k,object[k]);
			} else {
			  result += listValuesTable(k,k,object[k]);
			}
			
		}
		return result;
    }
    return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";
    
}

exports.object = function(req,res) {
	debug("exports.object");
	var db = res.db;
   
	var collectionName = req.params["collection"];
	var objid = req.params["id"];
	//console.log(objid);
	var object=ObjectID(objid);

	db.collection(collectionName).findOne ({_id:object},function handleFindOneObject(err, obj) {
		debug("handleFindOneObject");
		if (err) {
			var text = "Display Object "+ objid + " in Collection "+collectionName;
			text += "Error: "+JSON.stringify(err);
			res.set('Content-Type', 'text/html');
			res.end(text);
		} else {
			text = "<tr><th>Key</th><th>Value</th><tr>";
			text+= listValuesTable("",null,obj);
		
			page =new htmlPage.create("table");
			page.title = "Data Inspector";
			page.menu ="";
			page.content = '<h1>'+collectionName+'</h1><p><table>'+text+'</table></p>';
			res.set('Content-Type', 'text/html');
			console.dir({schluessel:obj.schluessel,
				                                          source: object});
			if (collectionName == "WorkerQueue") {
				db.collection("DataCollection").findOne ({schluessel:obj.schluessel,
				                                          source: obj.source},
				                                        function handleFindOneObject(err, obj2) {
					debug("handleFindOneObject2");
					if (err) {
						console.log(JSON.stringify(err));
						res.end(page.generatePage());
					} else {
					
						var text = "<tr><th>Key</th><th>Value</th><tr>";
						text+= listValuesTable("",null,obj2);
	
						
						page.content += '<h1>Zugehörige Daten</h1><p><table>'+text+'</table></p>';
						res.end(page.generatePage());
					}
		
						
			  
			})} else res.end(page.generatePage());
		}
	})}
	





function generateQuery(measure,schluessel,sub) {
	debug("generateQuery(%s,%s,%s)",measure,schluessel,sub);
   
	
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
  debug("generateQueryCSV(%s,%s)",measure,schluessel);
  var query = wochenaufgabe.map[measure].overpass.query;
  query = wochenaufgabe.map[measure].overpass.query;
  var fieldList = wochenaufgabe.map[measure].overpass.csvFieldList;
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
	debug("exports.overpass");
	var db = res.db;
   
	var measure = req.params["measure"];
	var schluessel = req.params["schluessel"];
	
	
	var sub = req.query.sub;
	if (typeof(sub) == 'undefined') sub = "";
    var query = generateQuery(measure,schluessel,sub);
    
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
	var page = htmlPage.create();
	page.content = text;
	
	res.set('Content-Type', 'text/html');		
    res.end(page.generatePage());
}

exports.importCSV = function(req,res){
	debug("exports.importCSV");
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
    	var defJSON = { measure: "AddrWOStreet",  
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
	debug("importApotheken");
    var db = res.db;
    var measure = req.params.measure;
    importCSV.importApothekenVorgabe(measure,db,function ready(err) {
 		var text;
		var text = "Importiert Apotheken "+measure+"<br>";
    	if (err) {
    		text += "Fehler: "+JSON.stringify(err);
    	} else {
			text += " File imported";
		}
		res.set('Content-Type', 'text/html');
		res.end(text);
    });
}
	
function gl(text, newLink, param)
{
	debug("gl");
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
	debug("setParams");
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
	debug("generateTable");
	debug(JSON.stringify(rank));
	
	var tableheader = "";
	var tablebody="";
	
	if (typeof(serviceLink)=='undefined') serviceLink = true;
	
	var sumrow = [];
	
	
	
	for (i=0;i<header.length;i++) {
		var cell = header[i];
		var celltext = cell;
		if (format[cell] && typeof(format[cell].title) != 'undefined') {
			celltext = format[cell].title;
		}
		if (cell == param.sort) {
			celltext = "#"+celltext+"#";
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
		var row = firstColumn[i];
		
		for (z=0;z<header.length;z++) {
			var col=header[z];
			
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
		var col = header[c];
		if (typeof(rank[col])!='undefined' && table[firstColumn[0]]) {
			var value = table[firstColumn[0]][col];
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
			var va = table[a][param.sort];
			var vb = table[b][param.sort];
			if (typeof(va)!='number') va = 0;
			if (typeof(vb)!='number') vb = 0;
			return (vb-va)*param.sortAscending});
	}
	for (i=0;i<firstColumn.length;i++)
	{
		var row = firstColumn[i];
		var tablerow = "";
		var line = table[row];
		
		for (z=0;z<header.length;z++) {
			var col=header[z];
			
			var cell;
			var content=table[row][col];
			var f = (format[col]) ? format[col].format: null;
			var glink = (format[col]) ? format[col].generateLink:null;
			
			if (typeof(content) == "undefined") {
				cell ="-";
			} else {
				if (f) {
					cell = util.numeral(content).format(f);
				} else if (typeof(content)=='number') {
					cell = util.numeral(content).format();
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
				cell = '<a href="'+glink(line["Schlüssel"])+'">'+cell+'</a>';
			}
			var cl ="";
			if ((typeof(rank[col])!="undefined")  && (rank[col]=="up") ){
			
				if (content == last[col]) cl = 'class = "last"';
				if (content == lastButOne[col]) cl = 'class = "lastButOne"';
				if (content == second[col]) cl = 'class = "second"';
				if (content == first[col]) cl = 'class = "first"';
			
			}
			if ((typeof(rank[col])!="undefined")  && (rank[col]=="down") ) {
			
				if (content == first[col]) cl = 'class = "last"';
				if (content == second[col]) cl = 'class = "lastButOne"';
				if (content == lastButOne[col]) cl = 'class = "second"';
				if (content == last[col]) cl = 'class = "first"';
			
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
	var tablerow = "";
	var line = sumrow;
	for (z=0;z<header.length;z++) {
		var col=header[z];
	
		var cell = "";
		if (z==0) cell = "Summe";
		var content=sumrow[col];
		var f = (format[col]) ? format[col].format: null;
		var glink = (format[col]) ? format[col].generateLink:null;
		
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
				cell = util.numeral(content).format(f);
			} else if (typeof(content)=='number') {
				cell = util.numeral(content).format();
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
	debug("generateCSVTable");
	
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
				cell = util.numeral(content).format('0,0.0'); 
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
	debug("generateFilterTable");

    var filterSub = "-"
    var filterSubPercent = "-";
    var subSelector = '';
    var subPercentSelector = "";
    if (  (param.measure == "Apotheke")
        ||(param.measure == "Apotheke_AT")
        ||(param.measure == "ApothekePLZ_DE")) { 
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
    
    
    
    var lokSelector = '<select name="lok"> ';
  
  	var lokMin = 2;
  	var lokMax = 11;
  	if (param.measure == "Apotheke_AT") {
  	   lokMin = 1;
  	   lokMax = 5;
  	}
  	if (param.measure == "ApothekePLZ_DE") {
  	   lokMin = 1;
  	   lokMax = 5;
  	  }
  	
    for (i=lokMin;i<=lokMax;i++) {
   		if (i == param.lengthOfKey) {
    		lokSelector += '<option value="'+i+'" selected>'+i+'</option>';
    	} else {
    		lokSelector += '<option value="'+i+'">'+i+'</option>';
    	}
    }

    
	var filterSelector = "";
    var filterText = "";
    if (param.location!="") {
    	var kreisnamen =  wochenaufgabe.map[param.measure].keyMap; 
    	
    	filterText+= param.locationName+" ("+param.location+")";
    	filterSelector += optionValue(param.location,filterText,param.location);
    	for (i=param.location.length-1;i>=lokMin;i--) {
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
	var date = new Date();
	var date7 = new Date();
	var date10 = new Date();
	var date14 = new Date();
	
	var date2 = date.getDate();
	date7.setDate(date2-7);
	date10.setDate(date2-10);
	date14.setDate(date2-14);
	
	var since = '';
	var since7 = date7.toISOString().substr(0,10);
	var since10 = date10.toISOString().substr(0,10);
	var sincex = "Wochenaufgabe";
	
	if ((param.since != since7) && (param.since != since) && (param.since!= since10)&& (param.since!= sincex)) {
		sincex = param.since;
	}
	
	
    var sinceSelector = '<select name="since"> \
			<option value="'+since+'"' +((param.since == since) ? " selected":"")+ '>'+since+'</option> \
			<option value="'+since7+'"' +((param.since == since7) ? " selected":"")+ '>'+since7+'</option> \
			<option value="'+since10+'"' +((param.since == since10) ? " selected":"")+ '>'+since10+'</option> \
			<option value="'+sincex+'"' +((param.since == sincex) ? " selected":"")+ '>'+sincex+'</option> \
			</select>';
    
	
	var filterTableL1, filterTableL2;
	
	// Filter on Location
	var filterTableLH = '<th class = "menu" > Ort </th>';
	var filterTableL1 = '<td class = "menu" >'+filterText+'</td>';
	var filterTableL2 = '<td class = "menu">'+filterSelector+'</td>';
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
	
	
	
	var filterTable = '<table class="menu"><tr>'+filterTableLH+'</tr> \
										<tr>'+filterTableL1+'</tr> \
										<tr>'+filterTableL2+'</tr></table>';
	
	
	//filterTable = "<tr><td>"+filterText+
	//filterTable = "<b>Gefiltert Auf:"+filterText + "</b> "+ filterSelector+ subSelector + subPercentSelector + periodenSelector + lokSelector + '<input type="submit" value="Parameter Umstellen">';
	filterTable = "<form>"+filterTable+"</form>";
	
	return filterTable;

}

function generateSortHeader(param,header,format) {
	debug("generateSortHeader");


    
    for (var i=0;i<header.length;i++) {
    	if (header[i]=="Admin Level") continue;
    	if (header[i]=="Name") continue;
    	var link = "";
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
	debug("exports.table");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection = db.collection('DataCollection');
    var collectionTarget = db.collection('DataTarget');
    

	


	var param = {};
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
    
    
   
    

    
    var ranktype =wochenaufgabe.map[param.measure].ranktype;

   
 
    var valueToCount = "$count";
   	if (param.sub != "") {
   		valueToCount = "$"+param.sub;
   		ranktype = "down";
   	}
   	
   	var valueToDisplay = "$count";
    
    if (param.subPercent == "Yes") {
    	valueToDisplay = { $cond : [ {$eq : ["$count",0]},0,{$divide: [ "$count","$total"]}]};
    	ranktype = "up";
    } 
    var paramSinceDate = new Date();
    paramSinceDate.setDate(paramSinceDate.getDate()-10*365);
    if (param.since != '') paramSinceDate = new Date(param.since);
    var preFilter = {$match: { timestamp: {$gte: paramSinceDate},
                                measure: param.measure,
                                schluessel: {$regex: "^"+param.location}}};
    var preFilterVorgabe = {$match: { 
                                measure: param.measure,
                                schluessel: {$regex: "^"+param.location}}};

	if (param.since == 'Wochenaufgabe') {
	  var date = new Date();
	  var date7 = new Date();
	  date7.setDate(date.getDate()-7);
	  
      var preFilter = {$match: { 
                                measure: param.measure,
                                schluessel: {$regex: "^"+param.location},
                                $and:[{timestamp: {$gte: paramSinceDate}},
                                 {$or:[{timestamp:{ $gte: date7}},
                                       
                                         {$and : [{timestamp: {$lte: new Date (2015,1,1)}},
                                                  {timestamp: {$gte: new Date (2015,0,31)}}]}
                                               ]}]}};

                                       
	  
	}    
    var projection = {$project:{measure:1,
                                timestamp:1,
                                schluessel:1,
                                count:1,
                                source:1,
                                "missing.name":1,
                                "missing.wheelchair":1,
                                "missing.phone":1,
                                "missing.opening_hours":1,
                                "existing.fixme":1}};
 
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
    
    
   
    	
    var query = [projection,
                 preFilter,
    			 aggregateMeasuresProj,
    			 aggregateMeasuresGroup,
    			 presort,
    			 aggregateTimeAxisStep2,
    			 sort];

	var queryVorgabe = [
				preFilterVorgabe,
				{$project: {  schluessel: { $substr: ["$schluessel",0,param.lengthOfKey]},
    						 			  vorgabe: "$apothekenVorgabe"
    						 			  }},
    			{$group: { _id:  "$schluessel",
    						 vorgabe	: {$sum: "$vorgabe" },
    						 		  }}];
    						 		  
    						 		  
	debug("query:"+JSON.stringify(query));
	debug("queryVorgabe:"+JSON.stringify(queryVorgabe));

    var aggFunc=query;   						
 	var openQueries=0;
 	var errorQueries=0;
 	var workingSchluessel="";
 	var workingTimestamp ="unknown";
 	var workingName="";
    var items = [];
    var vorgabe = {};
    async.parallel( [ 
    	function aggregateCollection(callback) {
    		debug("aggregateCollection");
			collection.aggregate(	query
								, (function aggregateCollectionCB(err, data) {
				debug("aggregateCollectionCB");
				// first copy hole table in a 2 dimensional JavaScript map
				// may be here is some performance potential :-)
				if (err) {
					res.set('Content-Type', 'text/html');
					res.end("Error in Aggregate: "+JSON.stringify(err));
					console.log("Table Function, Error occured:");
					console.log(err);
					;
				}
				items = data;
				callback(err);
			}))},
			function getVorgabe(callback) {
				debug("getVorgabe");
				if (   ((param.measure=="Apotheke" ) || 
				        (param.measure=="ApothekePLZ_DE" ) ||  
				        (param.measure == "Apotheke_AT"))
				    && param.sub =="") {
					collectionTarget.aggregate(	queryVorgabe
									, (function getVorgabeCB(err, data) {
					debug("getVorgabeCB");
					if (err) {
						res.set('Content-Type', 'text/html');
						res.end("Error in getVorgabe "+JSON.stringify(err));
						console.log("Table Function, Error occured:");
						console.log(err);
						;
					}
					for (var i = 0;i<data.length;i++)
					{
						schluessel = data[i]._id;
						value = data[i].vorgabe;
						vorgabe [schluessel]=value;
					}
					callback(err);
				}))
			} else callback();
			},
			function getWorkerQueueCount(callback) {
				debug("getWorkerQueueCount");
				db = res.db;
				var collectionName = 'WorkerQueue';
				
				// Fetch the collection test
				var collection = db.collection(collectionName);
				var date = new Date();
				collection.count({status:"open",
				                  exectime: {$lte: date},
				                  schluessel: {$regex: "^"+param.location},
				                  measure:param.measure},
								 function getWorkerQueueCountCB(err, count) {
					debug("getWorkerQueueCount");
					if (err)  {
					  openQueries = "#Error#";
					} else {
					  openQueries = count;
					}
					// Ignore Count Error, and 
					callback();
				});  
			},
			function getErrorQueueCount(callback) {
				debug("getErrorQueueCount");
				db = res.db;
				var collectionName = 'WorkerQueue';
				
				// Fetch the collection test
				var collection = db.collection(collectionName);
				var date = new Date();
				collection.count({status:"error",
								 exectime: {$lte: date},
								 schluessel: {$regex: "^"+param.location},
								 measure:param.measure},
								 function getWorkerQueueCountCB(err, count) {
					debug("getWorkerQueueCount");
					if (err) {
					  errorQueries = "#Error#"	
					} else {
					  errorQueries=count;
					}
					callback();
				});  
			},
			function getWorkingName(callback) {
				debug("getWorkingName");
				db = res.db;
				var collectionName = 'WorkerQueue';
				
				// Fetch the collection test
				var collection = db.collection(collectionName);
				var date = new Date();
				collection.findOne({status:"working",
				                    schluessel: {$regex: "^"+param.location},
				                    measure:param.measure},
								 function getWorkingNameCB(err, data) {
					debug("getWorkingNameCB");
					if (err) {
						workingSchluessel = "#Error get Working#";
						callback ();
						return;
					}
					if( data) {
					   if (data.type == "overpass") {
					      workingSchluessel = data.schluessel;
					      workingTimestamp = (new Date()-data.timestamp)/(1000*60); 
					      workingTimestamp = util.numeral(workingTimestamp).format('0.00');
					      var t =kreisnamen[workingSchluessel];
					      if (typeof(t)=='object') {
					      workingName = t.name;
					    }
					    if (data.type == "insert") {
					    	workingSchluessel = "Insert " + data.measure;
					    }
					  }
					}  
					 
					callback();
				});  
			}
			],
			function displayFinalCB (err, results ) {
				debug("displayFinalCB");
				// Initialising JavaScript Map
				var header = [];
				var firstColumn = [];
				var table =[]; 
				var format=[];
				var rank = [];
		
				//iterate total result array
				// and generate 2 Dimensional Table
				for (i=0;i < items.length;i++) {
			
			
					var measure = items[i];
					debug("Measure i"+i+JSON.stringify(measure));
					var row=measure._id.row;
					var col=measure._id.col;
			
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
				displayVorgabe = ( (param.lengthOfKey >= 1)
				                 &&((param.measure=="Apotheke") || 
				                    (param.measure=="ApothekePLZ_DE") ||
				                    (param.measure=="Apotheke_AT") )
				                 && (param.sub == ""));
		

				if (displayVorgabe) {
					header.unshift("Vorgabe");
					format["Vorgabe"] = {};
					format["Vorgabe"].toolTip = "theoretische Apothekenzahl";
					format["Vorgabe"].sum = true;
					format["Vorgabe"].format = '0,0.0';
					format["Vorgabe"].title = 'Schätzung';
					format["Vorgabe"].generateLink = function(value) {
					     return "/list/DataTarget.html?measure="+param.measure+"&schluessel="+value;}
			
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
						
						var waApothekeStart = "2015-01-31"
						if (header.indexOf(waApothekeStart) >=0) {
							format["Diff"].title = "WA Diff";
							format["Diff"].func.op2=waApothekeStart;
							format["Diff"].toolTip = "Differenz zwischen "+ header[header.length-2]+ " und Wochenaufgabenstart (31.1.15)";
					}
					
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
						format[colName].func.numerator = header[header.length-3];
			
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
					
					var pageFooter = "";
					var separator = "";
					if (openQueries > 0) {
						pageFooter += '<a href="/list/WorkerQueue.html?'
						               +'measure='+ param.measure
						               +'&type=overpass&status=open'+
						               '"><b>Offene Queries: '+openQueries+'.</b></a> ';
						separator = "<br>";
					} 
					if (errorQueries > 0) {
							separator = "<br>";
							pageFooter += '<a href="/list/WorkerQueue.html?'
						               +'measure='+ param.measure
						               +'&type=overpass&status=error'+
						               '"><b>Fehler: '+errorQueries+'.</b></a> ';
					}
					if (workingSchluessel !="") {
						separator = "<br>";
						pageFooter += "Lade: "+ workingName + " ("+workingSchluessel+") seit "+workingTimestamp + " Minuten";
					}
					{
						separator = "<br>";
							pageFooter += '<a href="/list/WorkerQueue.html?'
						               +'measure='+ param.measure
						               +'&type=insert&'+
						               '">(Zeitplan)</a> ';
					}
					pageFooter += separator;
					pageFooter += "Die Service Links bedeuten: \
									<b>O</b> Zeige die Overpass Query \
									<b>R</b> Starte die Overpass Query \
									<b>#</b> Öffne Overpass Turbo mit CSV Abfrage"
					page.footer = pageFooter;
						
					debug(JSON.stringify(query,null,' '));
					
					res.set('Content-Type', 'text/html');
					res.end(page.generatePage());
					return;
				} 
				if (param.csv) {
					var table = generateCSVTable(param,header,firstColumn,table,";");
					
					if (openQueries > 0 ) {
						table = "Achtung: Es laufen noch "+openQueries+" Overpass Abfragen, das Ergebnis ist eventuell unvollständig\n"+table;
					}					
					if (errorQueries > 0 ) {
						table = "Achtung: Es liegen "+errorQueries+" Overpass Fehler vor, das Ergebnis ist eventuell unvollständig\n"+table;
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
  //console.log("columns:"+JSON.stringify(columns));
  //console.log("object:"+JSON.stringify(object));
  //console.log("d:"+d);
  if (typeof(object)=='string') return object;
  if (typeof(object)=='undefined') return "";

  if (typeof(columns) == 'object') {
  	//console.log("typeof(columns) == 'object'");
  	//console.dir(columns[d]);
    if (typeof(columns[d])=='string') {
      //console.log("typeof(columns[d])=='string'");
      return getValue(columns,object[columns[d]],d+1);
    }
    else {
       //console.log("else");
    	var result = "";
    	for (var i =d;i<columns.length;i++) {
    	    
    		var result2 = getValue(columns[i],object,0);
    		if (result != "" && result2 != "") {
    		  result = result + "," + result2;
    		} else result += result2;
    	}
    	return result;
    }

   	if (typeof(object[columns[d]])=='object') {
   		
   	} else {
   	
   		result = object[columns[d]];
   	}
   } else {
   	result = object[columns];
   }
   return (typeof(result)=='undefined')?"":result;
}


exports.query=function(req,res) {
	debug("exports.query");
	var db = res.db;
  
    // Fetch the collection DataCollection
    // To be Improved with Query or Aggregation Statments
    var collection;
    var query;
    var options={};
    var queryMenu = "";
    var queryDefined = false;
    switch (req.params.query) {
    	case "DataTarget": collection = db.collection('DataTarget');
    						collectionName = "DataTarget";
    	               columns = ["_id",
    	                          "measure",
    	                          "schluessel",
    	                          "name",
    	                          "apothekenVorgabe",
    	                          "source",
    	                          "linkSource"];
    	               query = {};
    	               if (typeof(req.query.measure) != 'undefined'){
    	               	query.measure = req.query.measure;
    	               } 
    	               if (typeof(req.query.schluessel) != 'undefined'){
    	               	query.schluessel = {$regex: "^"+req.query.schluessel};
    	               } 
    	               options={"sort":"schluessel"}
    	               break;
    	case "WorkerQueue": collection = db.collection('WorkerQueue');
    						collectionName = "WorkerQueue";
    	               columns = ["_id",
    	                          "type",
    	                          "status",
    	                          "measure",
    	                          "schluessel",
    	                          "prio",
    	                          "exectime",
    	                          "timestamp",
    	                          "error",
    	                          ["Error Code","error","code"],
    	                          ["Error Status Code","error","statusCode"]];
    	               query = {};
    	               if (typeof(req.query.type) != 'undefined'){
    	               	query.type = req.query.type;
    	               } 
    	               if (typeof(req.query.status) != 'undefined'){
    	               	query.status = req.query.status;
    	               } 
    	               if (typeof(req.query.measure) != 'undefined'){
    	               	query.measure = req.query.measure;
    	               } 
     	               
     	               options={"sort":"exectime"}
    	               break;
    	case "pharmacy": collection = db.collection('POI');
    	                 collectionName = "POI";
    	                  columns = ["Links",
    	                             ["name","tags","name"],
    	                             ["state","nominatim","state"],
    	                             ["state_destrict","nominatim","state_district"],
    	                             ["county","nominatim","county"],
    	                             ["PLZ","nominatim","postcode"],
    	                             ["Town",["nominatim","town"],["nominatim","village"],["nominatim","city"]],
    	                             ["Straße",["nominatim","road"],["nominatim","pedestrian"]],
    	                             ["Hausnummer","nominatim","house_number"],
    	                             ["Öffnungszeiten","tags","opening_hours"],
    	                             ["Operator","tags","operator"],
    	                             ["Telefon",["tags","phone"],["tags","contact:phone"]],
    	                             ["Fax",["tags","fax"],["tags","contact:fax"]],
    	                             ["website",["tags","website"],["tags","contact:website"]],
    	                             ["wheelchair","tags","wheelchair"],

    	                             ];
    	                  query = {};options={}; 
    	                  queryMenu = "";
    	                  
    	                  if (typeof(req.query.state)!='undefined' && req.query.state != "") {
    	                    queryDefined = true;
    	                    query["nominatim.state"] =req.query.state;
    	                    queryMenu += 'state:<input type="text" name="state" value="'+req.query.state+'">';
    	                  } else queryMenu += 'state:<input type="text" name="state">';

   	                      if (typeof(req.query.state_district)!='undefined' && req.query.state_district != "") {
    	                    queryDefined = true;
    	                    query["nominatim.state_district"] =req.query.state_district;
    	                    queryMenu += 'state_district:<input type="text" name="state_district" value="'+req.query.state_district+'">';
    	                  } else queryMenu += 'state_district:<input type="text" name="state_district">';

   	                      if (typeof(req.query.county)!='undefined' && req.query.county != "") {
    	                    queryDefined = true;
    	                    query["nominatim.county"] =req.query.county;
    	                    queryMenu += 'county:<input type="text" name="county" value="'+req.query.county+'">';
    	                  } else queryMenu += 'county:<input type="text" name="county">';
 
    	                  if (typeof(req.query.postcode)!='undefined' && req.query.postcode != "") {
    	                    queryDefined = true;
    	                    query["nominatim.postcode"] =req.query.postcode;
    	                    queryMenu += 'postcode:<input type="text" name="postcode" value="'+req.query.postcode+'">';
    	                  } else queryMenu += 'postcode:<input type="text" name="postcode">';
    	                  
    	                  
     	                  if (typeof(req.query.city)!='undefined' && req.query.city != "") {
    	                    queryDefined = true;
    	                    var q = [{"nominatim.town":req.query.city},
    	                             {"nominatim.city":req.query.city},
    	                             {"nominatim.village":req.query.city}];
    	                    query["$or"] =q;
    	                    queryMenu += 'town:<input type="text" name="city" value="'+req.query.city+'">';
    	                  } else queryMenu += 'town:<input type="text" name="city">';
    	                  
    	                  if (typeof(req.query.country)!='undefined' && req.query.country != "") {
    	                    queryDefined = true;
    	                    query["nominatim.country"] =req.query.country;
    	                    queryMenu += 'country:<input type="text" name="country" value="'+req.query.country+'">';
    	                  } else queryMenu += 'country:<input type="text" name="country">';
    	                  
    	                  if (typeof(req.query.missing)!='undefined' && req.query.missing != "") {
    	                    queryDefined = true;
    	                    query["nominatim.timestamp"] ={$exists:0};
    	                    query["overpass.loadBy"] = req.query.missing;
     	                  } 
     	                  queryMenu += 'missing nominatim: <select name="missing"> \
    	                                   <option value="">none</option> \
    	                                  <option value="DE">DE</option> \
    	                                  <option value="AT">AT</option> \
    	                                  <option value="CH">CH</option> \
    	                                  </select>';
 
    	                  
    	                  
    	                
    	   
    	                  queryMenu = '<form>'+queryMenu+'<input type="submit"></form>';
    	                  break;
    	 default:collection = db.collection('WorkerQueue');
    	               columns = ["_id","type","status","measure"];
    	               query = {type:"insert"};
    }
    //console.dir(query);
    if (collectionName != "POI" || queryDefined) {
    collection.find(query,options).toArray(function(err,data) {
    	if (err) {
    		res.set('Content-Type', 'text/html');
			res.end(JSON.stringify(err)+JSON.stringify(query));
			
 	   		console.log("Error: "+err);
 	   		return;
    	}
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
    	for (i = 0;i<data.length;i++) {
    	  tablerow = "";
    	  d = data [i];   
    	  tablerow = '<td> <a href="/object/'+collectionName+'/'+d._id+'.html">'+d._id+'</a></td>';
    	  if (req.params.query == "pharmacy") {
    	    link1 = '<a href="/object/'+collectionName+'/'+d._id+'.html">Data</a>';
    	    link2 = '<a href="https://www.openstreetmap.org/'+d.type+'/'+d.id+'">OSM</a>';
    	     tablerow = '<td>'+link1+"  "+ link2+'</td>';
    	  }
    	  key = columns[j];
    	  for (j=1;j<columns.length;j++) {
      		tablerow += "<td>"+getValue(columns[j],d,1);+"</td>";
    	  }
    	  tablerow = "<tr>"+tablerow+"</tr>";
    	  table += tablerow;
    	}
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.menu =queryMenu;
		page.content = '<p><table>'+table+'</table></p>';
		page.footer = "Ergebnisse: " + data.length + " Abfrage: "+JSON.stringify(query)+"<br>Aktuelle Zeit: "+new Date();
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;
   
    })
    } else {
    	page =new htmlPage.create("table");
		page.title = "Abfrage "+req.params.query;
		page.menu =queryMenu;
		page.content = '<p>Bitte die POIs durch eine Query Einschränken</p>';
		page.footer = JSON.stringify(query);
 		res.set('Content-Type', 'text/html');
 		res.end(page.generatePage());
 		return;
    
    }
    
    
}
    
    

	

