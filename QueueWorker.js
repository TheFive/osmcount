var config = require('./configuration.js');
var lod = require('./LoadOverpassData.js');
var util = require('./util.js')
var async=require('async');
var debug         = require('debug')('QueueWorker');
 debug.entry   = require('debug')('QueueWorker:entry');
 debug.data    = require('debug')('QueueWorker:data');





function getNextJob(cb,job) {	
	debug.entry("getNextJob("+cb+","+job+")");
	var date= new Date();
	var dateJSON = date.toJSON();
	mongodb = config.getDB();
	debug.data("Current Time %s",dateJSON);
	mongodb.collection("WorkerQueue").findOne( 
							{ status : "open" , 
							 exectime: {$lte: date}
							}, function(err, obj) 
	{
		debug.entry("getNextJob callback("+err+","+obj+")");
		if (err) debug(err);

		debug.data("found job %s",obj.type);		
		// return obj as job object
		cb(err,obj);
	}
)}

function saveMeasure(result,cb) {
	debug.entry("saveMeasure("+result+","+cb+")");

	if (result==null) {
		cb();
		return;
	}
	mongodb.collection("DataCollection").save(result,{w:1}, function (err, num){
		debug.entry("saveMeasure callback("+err+","+num+")");
	    if (err) {
	     	debug(err);
	     	cb()
	     	return;
		}
		cb();
		return;
	}	
)}

function saveJobState(newState,oldState) {
	debug.entry("Generate: saveJobState(%s,%s)",newState, oldState);
	return function (cb,job) {
			debug.entry("Execute:saveJobState Function(%s,%s)("+cb+","+job+")",newState, oldState);
			job = job.readjob;
			if (job==null || job.status == 'undefined' || job.status != oldState) {
				debug.data("Nothing to execute");
				cb(null,job);
				return;
			}
			date = new Date();
			job.timestamp = date;
			job.status = newState;
			debug("Setting Jobsstatus to %s",newState);
			mongodb.collection("WorkerQueue").save(job,{w:1}, function (err, num){
				debug.entry("Execute CB:saveJobState Function(%s,%s)("+cb+","+job+")",err, num);
				if (err) {
					debug(err);
				} else {
					
				}
				cb(err,job);
			}	
	)}
}

// Creating async queue and start to initialise Database
// (should be done in server in future)
var q= async.queue(function (task,cb) {
	debug.entry("async.queue Start Next Task");
	task(cb);
},1);

function doConsole(cb,job) {
	debug.entry("doConsole("+cb+","+job+")");
	if (typeof(job.status)!='undefined' && job.status =="working" && job.type == "console") {
		debug(job.text);
		console.log(job.text),
		job.status = "done";
	}  
	cb(null,job);	
}


function doOverpass(cb,job) {
	debug.entry("doOverpass("+cb+","+job+")");
	if (typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpass") {
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
					cb(err,job);
				}
				
			)		
	} else cb(null,job);
}



function doInsertJobs(cb,job) {
	debug.entry("doInsertJobs("+cb+","+job+")");
	if (typeof(job.status)!='undefined' && job.status =="working" && job.type=="insert") {
		debug("Start Insert");
		mongodb = config.getDB();
		jobs = lod.createQuery("AddrWOStreet");
		mongodb.collection("WorkerQueue").insert(jobs,
			function (err, records) {
				if (err) console.log(err);
				debug("All Inserted %i" ,records.length);
				cb(err,job);
			})
	}
}




function doNextJob(callback) {
	debug.entry("doInsertJobs("+callback+")");
	job = {};
	async.auto( {readjob:     getNextJob,
		    saveWorking: ["readjob",saveJobState("working","open")],
		    doConsole:   ["saveWorking", doConsole],
		    doOverpass:  ["saveWorking", doOverpass],
		    doInsertJobs:["saveWorking", doInsertJobs],
		    saveDone:    ["doConsole","doOverpass", saveJobState("done","working")],
		    saveError:   ["saveDone", saveJobState("error","working")],
		},
		function () {
			debug("finished %s" ,job.status);
			if (typeof(job.status)== 'undefined') {
				q.push(util.waitOneMin);
			}
			q.push(doNextJob);
			callback();
		}
	)
}

exports.startQueue =function(cb) {
	debug.entry("startQueue("+cb+")");
	q.push(config.initialiseDB);
	q.push(doNextJob);
	if (cb) cb();
}










