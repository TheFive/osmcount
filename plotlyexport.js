var plotly        = require('plotly')('thefive.osm','8c8akglymf');
var debug         = require('debug')('plotyexport');
var async         = require('async');

var htmlPage       = require('./htmlPage.js');
var configuration  = require('./configuration.js');
var wochenaufgabe  = require('./wochenaufgabe.js');

var loadDataFromDB = require('./model/LoadDataFromDB.js');
var DataCollection = require('./model/DataCollection.js');



function getKreisname(schluessel,kreisnamen) {
	if (schluessel =="") return "Deuschland";
  	if (typeof(kreisnamen[schluessel])=='undefined') return schluessel;
	n = kreisnamen[schluessel].name;
	n = n.replace("ü","ue");
	n = n.replace("ö","oe");
	n = n.replace("ä","ae");
	n = n.replace("ß","ss");
	n = n.replace("Ü","Ue");
	n = n.replace("Ö","Oe");
	n = n.replace("Ä","Ae");
	n = n.replace("ü","ue");
	n = n.replace("ö","oe");
	n = n.replace("ä","ae");
	n = n.replace("ß","ss");
	n = n.replace("Ü","Ue");
	n = n.replace("Ö","Oe");
	n = n.replace("Ä","Ae");
	n = n.replace("è","e");
	n = n.replace("è","e");
	n = n.replace("è","e");
	n = n.replace("â","a");
	n = n.replace("â","a");

    return n;
}

exports.plot = function(req,res){
	debug("exports.plot");

    var param = {}
	param.measure = req.params.measure;
    param.location = req.query.location;
    if (typeof(param.location) =='undefined') param.location="";
    var lok = req.query.lok;
    param.lengthOfKey = 2;
    if (typeof(parseInt(lok))=='number') param.lengthOfKey = parseInt(lok);

    
  var kreisnamen =  wochenaufgabe.map[param.measure].map.map;
  var keyList = wochenaufgabe.map[param.measure].map.keyList;
  if (typeof(kreisnamen)=='undefined') kreisnamen = keyList;
  if (typeof(kreisnamen)=='undefined') kreisnamen = {};

 
    var items = [];
    async.parallel( [
    	function aggregateCollection(callback) {
    		debug("aggregateCollection");
			DataCollection.aggregate(	param
								, (function aggregateCollectionCB(err, data) {
				debug("aggregateCollectionCB");
				// first copy hole table in a 2 dimensional JavaScript map
				// may be here is some performance potential :-)
				if (err) {
					res.set('Content-Type', 'text/html');
					res.end("error"+err);
					console.log("Table Function, Error occured:");
					console.log(err);
				}
				items = data;
				callback(err);
			}))}],
			function displayFinalCB (err, results ) {
				bundeslandRow = {};
				debug("displayFinalCB");
				// Initialising JavaScript Map
				//iterate total result array
				// and generate 2 Dimensional Table
				for (i=0;i < items.length;i++) {


					measure = items[i];
					debug("Measure i"+i+JSON.stringify(measure));
					schluessel=measure._id.row;
					datum=measure._id.col;
					cell=parseFloat(measure.cell);



					//generate new Header or Rows and Cell if necessary
					if (typeof(bundeslandRow[schluessel])=='undefined') {

						bundeslandRow[schluessel]={};
						bundeslandRow[schluessel].x = [];
						bundeslandRow[schluessel].y = [];
						bundeslandRow[schluessel].name = getKreisname(schluessel,kreisnamen);
						bundeslandRow[schluessel].type = "scatter";
					}
					bundeslandRow[schluessel].x.push(datum);
					bundeslandRow[schluessel].y.push(cell);
				}
				data = [];
				for (key in bundeslandRow) {
					data.push(bundeslandRow[key]);
				}
				graphLocation = getKreisname(param.location,kreisnamen);

				filenamePlotly = "OSMWA_"+param.measure+"_"+graphLocation+"_"+param.lengthOfKey;
				title = "--";
				switch (param.measure) {
					case "Apotheke": title =  "Anzahl Apotheken in OSM fuer "+graphLocation;
					            break;
					case "AddrWOStreet": title = "Adressen ohne Strasse fuer "+graphLocation;
								break;
					default: title = "--";

				}
				var style = {title: title};

				var graphOptions = {filename: filenamePlotly, fileopt: "overwrite",layout:style};
				plotly.plot(data, graphOptions, function (err, msg) {

    			res.set('Content-Type', 'text/html');
    			if (!err) {
    				page = htmlPage.create("table");
    				page.content='<iframe width="1200" height="600" frameborder="0" seamless="seamless" scrolling="no" src='+msg.url+'.embed?width=1200&height=600"></iframe>';
    				page.footer = "Diese Grafik wird durch den Aufruf dieser Seite aktualisiert, der plot.ly Link kann aber auch unabh&aumlnig genutzt werden.";
    				res.end(page.generatePage());
    			} else {
    				res.end("Fehler von Plotly: "+ JSON.stringify(err));
    			}
				});

			}

		)

}


