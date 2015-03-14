var debug = require('debug')('displayAggregationTable');
var async = require('async');


var util          = require('../util.js');
var htmlPage      = require('../htmlPage.js');
var wochenaufgabe = require('../wochenaufgabe.js');
var DataCollection = require('../model/DataCollection.js')



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
    	var kreisnamen =  wochenaufgabe.map[param.measure].map.map;

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

function optionValue(value,displayValue,selected) {
	if (selected == value) {
		return '<option value ="'+value+'" selected><b>'+displayValue+'</b></option>';
	} else  {
		return '<option value ="'+value+'">'+displayValue+'</option>';
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
	// To be Improved with Query or Aggregation Statments
  var collectionTarget = db.collection('DataTarget');
  var timeAggregate;
  var timeVorgabe;
  var timeTableGeneration;

  var param = {};
  setParams(req,param);


    if (typeof(wochenaufgabe.map[param.measure])=='undefined')
    {
    	res.set('Content-Type', 'text/html');
		res.end("Die Wochenaufgabe "+param.measure+ " ist nicht definiert.");
    	return;
    }

 	var kreisnamen =  wochenaufgabe.map[param.measure].map.map;





    v = kreisnamen[param.location];
	if (typeof(v)!='undefined') {
		param.locationName=v.name;
		param.locationType=v.typ;
	}






    var ranktype =wochenaufgabe.map[param.measure].ranktype;



    param.valueToCount = "$count";
   	if (param.sub != "") {
   		param.valueToCount = "$"+param.sub;
   		ranktype = "down";
   	}

    var paramSinceDate = new Date();
    paramSinceDate.setDate(paramSinceDate.getDate()-10*365);



	var preFilterVorgabe = {$match: {
															measure: param.measure,
															schluessel: {$regex: "^"+param.location}}};



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
	param.upTo = '';
    var items = [];
    var vorgabe = {};
    async.parallel( [
    	function aggregateCollection(callback) {
    		debug("aggregateCollection");
    		timeAggregate = new Date().getTime();
			  DataCollection.aggregate(	param
								, (function aggregateCollectionCB(err, data) {
				debug("aggregateCollectionCB");
				timeAggregate = new Date().getTime() - timeAggregate;
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
				timeVorgabe = new Date().getTime();
				if (   ((param.measure=="Apotheke" ) ||
				        (param.measure=="ApothekePLZ_DE" ) ||
				        (param.measure == "Apotheke_AT"))
				    && param.sub =="") {
					collectionTarget.aggregate(	queryVorgabe
									, (function getVorgabeCB(err, data) {
					debug("getVorgabeCB");
					timeVorgabe = new Date().getTime() - timeVorgabe;
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
				timeTableGeneration = new Date().getTime();
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

					// Mongo DB Variant, has to be consolidated
					var row=measure._id.row;
					var col=measure._id.col;

					if (typeof(row)=='undefined' || typeof(col) == 'undefined') {
						debug("row or col undefined");
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
				for (i=0;i<firstColumn.length;i++) {
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



// -------------------------------------------------------
// Start Rendering the Page
// Has to be moved someway to the views
//
				if (param.html) {
					page =new htmlPage.create("table");
					page.title = "Wochenaufgabe "+param.measure;
					page.menu = generateFilterTable(param,header);

					generateSortHeader(param,header,format);
					var table = generateTable(param,header,firstColumn,table,format, rank);
					timeTableGeneration = new Date().getTime() - timeTableGeneration;

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
									<b>#</b> Öffne Overpass Turbo mit CSV Abfrage";
					pageFooter += "<br>Dauer Aggregate Funktion: "+(timeAggregate/1000)+ "s. Dauer Vorgaben: "+(timeVorgabe/1000)+"s. Dauer Aufbereitung: "+(timeTableGeneration/1000)+"s.";
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

			})

}