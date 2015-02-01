var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('test');
  debug.data = require('debug')('test:data');
  debug.entry = require('debug')('test:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js');
var request = require('request');
var ObjectID = require('mongodb').ObjectID;


var object=ObjectID("54bbda23a9fff29864d116b4");

var async    = require('async');

function readError(cb,result) {


configuration.getDB().collection("DataCollection").find( 
							{source:object,
							$and: 
                         [{measure: {$ne:"Apotheke"}},{measure:{$ne:"AddrWOStreet"}}]}).toArray( 
								function(err,data) {
  if (err) {
    console.log("Error: "+JSON.stringify(err));
    return;
  }
  console.log("Fehler gefunden: "+data.length);
  var schluesselList = [];
  for (i=0;i<data.length;i++) {
  	d = data[i];
  	schluesselList.push(d.schluessel);
  }			
  cb(null,schluesselList);					
								
});}

async.auto( {db:configuration.initialiseDB,
	         error:["db",readError]},
	         function (cb,result) {
	         	console.dir(result.error);
	         	cb();
	         }
             
             );