var plotly = require('plotly')('thefive.osm','8c8akglymf');
var debug    = require('debug')('test');
  debug.data = require('debug')('test:data');
  debug.entry = require('debug')('test:entry');
var configuration = require('./configuration.js');
var loadDataFromDB = require('./LoadDataFromDB.js');
var loadOverpassData = require('./LoadOverpassData.js');
var request = require('request');
var ObjectID = require('mongodb').ObjectID;
var wochenaufgabe = require('./wochenaufgabe.js');


var object=ObjectID("54bbda23a9fff29864d116b4");

var async    = require('async');

function correctMissingError(cb,result) {
  configuration.getMongoDB().collection("DataCollection").find(
							      {"missing.name":{$exists:0},measure:"Apotheke"}).each(
								function(err,result) {
  if (err) {
    console.log("Error: "+JSON.stringify(err));
    return;
  }
  if (result== null) return;
  //console.log("Vorher: Result");
  //console.dir(result);
  result.count=0;
  result.count = result.data.length;
  var tagCounter = wochenaufgabe.map[result.measure].tagCounter);
  if (tagCounter) tagCounter(result.data,result);

  //console.log("Nacher: Result");
  //console.dir(result);
   console.log("saved call");
  configuration.getMongoDB().collection("DataCollection").save(result,{w:1},function(err,r){
   console.log("saved called");
  	if (err) {
  		console.log("error during Save");
  		console.dir(err);
  	} else {
  		console.log("Updated: "+r);
  	}
  	});
});}



async.auto( {db:configuration.initialiseDB,
	         error:["db",correctMissingError]},
	         function (cb,result) {
	         	console.dir(result.error);
	         	cb();
	         }
