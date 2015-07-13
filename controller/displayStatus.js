var debug = require('debug')('displayStatus');
var async = require('async');

var htmlPage     = require('../htmlPage.js');

var WorkerQueue = require('../model/WorkerQueue.js');
var DataCollection = require('../model/DataCollection.js');
var POI = require('../model/POI.js');



exports.status = function(req,res) {
	debug("exports.status");
	var openQueries;
	var errorQueries;



	async.auto(
	{
		"Open Queries": function(cb) {WorkerQueue.count({status:"open"},cb);},
		"Error Queries": function(cb) {WorkerQueue.count({status:"error"},cb);},
		"Open Queries Until Now": function(cb) {WorkerQueue.countUntilNow({status:"open"},cb);},
		"Aggregate Cache":function(cb) {
			var a = DataCollection.aggregateCache;

			var r = {length:a.length,itemCount:a.itemCount,_max:a._max}
			cb(null,r)},
		"Missing Nominatim POI": function(cb) {POI.count("where ((data->'nominatim'->'timestamp') is  null)",cb);},
		"Total POI": function(cb) {POI.count(" ",cb);}
		

	}, function(err,result) {
		var page = htmlPage.create();
		page.title = "OSM Count Status"
		var content;
		content = "<h2>Result Status Object: </h2>";
		content += "<pre>"+JSON.stringify(result,null,2)+"</pre>";
		content += "<h2>Error Object (Für Statusabfrage)</h2>";
		content += "<pre>"+JSON.stringify(err,null,2)+"</pre>";

		page.content = content;
		res.set('Content-Type', 'text/html');
		res.end(page.generatePage());
	}
	)
}
