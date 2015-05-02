var debug = require('debug')('displayStatus');
var async = require('async');

var htmlPage     = require('../htmlPage.js');

var WorkerQueue = require('../model/WorkerQueue.js');
var DataCollection = require('../model/DataCollection.js');



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
			cb(null,r)}
		

	}, function(err,result) {
		var page;
		page = "Result Status Object: \n";
		page += JSON.stringify(result,null,2);
		page += "\n\nError Object\n";
		page += JSON.stringify(err,null,2);
		res.set('Content-Type', 'text/html');
		page = "<pre>"+page+"<pre>";
		res.end(page);
	}
	)
}
