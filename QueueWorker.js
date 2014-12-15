var config = require('./configuration.js');
var lod = require('./LoadOverpassData.js');
var util = require('./util.js')
var async=require('async');

mongodb = config.getDB();



function getNextJob(job,cb) {	
	var date= new Date();
	var dateJSON = date.toJSON();
	console.log(dateJSON);
	mongodb = config.getDB();
	mongodb.collection("WorkerQueue").findOne( 
							{ status : "open" , 
							 exectime: {$lte: date}
							}, function(err, obj) 
	{
	   if (err) {
	    	console.log(err)
	    	return;
	   }
	   util.copyObject(job ,obj);
	   cb();
	}
)}

function saveMeasure(result,cb) {
	if (result==null) {
		cb();
		return;
	}
	mongodb.collection("DataCollection").save(result,{w:1}, function (err, num){
	    if (err) {
	     	console.log(err);
	     	cb()
	     	return;
		}
		cb();
		return;
	}	
)}


function saveJobState(job,cb) {
	if (job==null) {
		cb();
		return;
	}
	mongodb.collection("WorkerQueue").save(job,{w:1}, function (err, num){
	    if (err) {
	     	console.log(err);
	     	cb()
	     	return;
		}
		cb();
		return;
	}	
)}

// Creating async queue and start to initialise Database
// (should be done in server in future)
var q= async.queue(function (task,cb) {
	console.log("Start next task");
	task(cb);
},1);






function doNextJob(callback) {
	job = {};
	async.series ( [
		function (callback) {
			getNextJob(job,callback);
		},
		function (callback) {

			if (typeof(job.status)!='undefined' && job.status =="open") {
				//console.dir(job);
				job.status="working";
				saveJobState(job,callback);
			} else callback();
			
		},
		function (callback) {
			if (typeof(job.status)!='undefined' && job.status =="working" && job.type == "console") {
	    		console.dir(job.text);
	    		job.status = "done";
			}  
			callback();
			
		},
		function (callback) {
			if (typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpass") {
		    		//console.dir(job.text);
		    		measure=job.measure;
		    		query=job.query;
		    		result= {};
		    		result.schluessel = job.schluessel;
		    		async.series( [
		    			function (cb) {
		    				lod.runOverpass(query,measure,result,cb);
		    			},
		    			function (cb) {
		    				saveMeasure(result,cb);
		    				
		    			}],
		    			function (err,ergebnis) {
		    				job.status="done";
		    				callback();
		    			}
		    			
		    		)		
			} else callback();
		},
		function (callback) {
			if (typeof(job.status)!='undefined') { 
				if (job.status=="working") {
					job.status="error";
				}
				date = new Date();
				job.timestamp = date;
				console.dir(job);
				saveJobState(job,callback);
			} else callback();
		}],
		function () {
			console.log("finished" + job.status);
			if (typeof(job.status)== 'undefined') {
				q.push(util.waitOneMin);
			}
			q.push(doNextJob);
			callback();
		}
	)
}

exports.startQueue =function() {
	console.log("Start Queue");
	q.push(config.initialiseDB);
	q.push(doNextJob);
}


function insertNextJob(callback) {
	var date= new Date();
	var dateJSON = date.toJSON();
	mongodb = config.getDB();

}


insertJobsToQueue = function(callback) {
	console.log("Start Insert");
	mongodb = config.getDB();
	mongodb.collection("WorkerQueue").insert(LoadOverpassData.createQuery("AddrWOStreet"),
		function (err, records) {
			console.log ("All Inserted");
			if (callback) callback();
		})
}

exports.insertJobs = function() {
	q.push(insertJobsToQueue);
}





