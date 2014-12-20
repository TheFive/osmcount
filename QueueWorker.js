var config = require('./configuration.js');
var lod = require('./LoadOverpassData.js');
var util = require('./util.js')
var async=require('async');
var debug        = require('debug')('QueueWorker');
 debug.entry   = require('debug')('QueueWorker:entry');
 debug.data    = require('debug')('QueueWorker:data');





function getNextJob(cb,job) {	
	debug.entry("getNextJob("+cb+","+job+")");
	var date= new Date();
	var dateJSON = date.toJSON();
	mongodb = config.getDB();
	debug.data("Current Time %s",dateJSON);
	debug.entry("getNextJob->call CB");
	mongodb.collection("WorkerQueue").findOne( 
							{ status : "open" , 
							 exectime: {$lte: date}
							}, function(err, obj) 
	{
		debug.entry("getNextJob->CB("+err+","+obj+")");
		if (err) debug(err);

		if (obj) debug.data("found job %s",obj.type);		
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
	debug.entry("saveMeasure->call CB");
	mongodb.collection("DataCollection").save(result,{w:1}, function (err, num){
		debug.entry("saveMeasure->CB("+err+","+num+")");
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
			debug.entry("saveJobState Function(%s,%s)("+cb+","+job+")",newState, oldState);
			job = job.readjob;
			if (job) debug.data("job.status=%s",job.status);
			if (job==null || job.status == 'undefined' || job.status != oldState) {
				debug.data("Nothing to execute");
				cb(null,job);
				return;
			}
			date = new Date();
			job.timestamp = date;
			job.status = newState;
			debug("Setting Jobsstatus to %s",newState);
			debug.entry("saveJobState(%s,%s)->call CB",newState,oldState);
			mongodb.collection("WorkerQueue").save(job,{w:1}, function (err, num){
				debug.entry("saveJobState(%s,%s)->CB("+err+","+num+")",newState, oldState);
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

function doConsole(cb,results) {
	debug.entry("doConsole("+cb+","+results+")");
	job=results.readjob;
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type == "console") {
		debug(job.text);
		console.log(job.text),
		job.status = "done";
	}  
	cb(null,job);	
}


function doOverpass(cb,results) {
	debug.entry("doOverpass("+cb+","+results+")");
	job=results.readjob;
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpass") {
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



function doInsertJobs(cb,results) {
	debug.entry("doInsertJobs("+cb+","+results+")");
	job=results.readjob;
	
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="insert") {
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
	else if (cb) cb(null,job);
}




function doNextJob(callback) {
	debug.entry("doNextJob("+callback+")");
	async.auto( {readjob:     getNextJob,
		    saveWorking: ["readjob",saveJobState("working","open")],
		    doConsole:   ["saveWorking", doConsole],
		    doOverpass:  ["saveWorking", doOverpass],
		    doInsertJobs:["saveWorking", doInsertJobs],
		    saveDone:    ["doConsole","doOverpass","doInsertJobs", saveJobState("done","working")],
		    saveError:   ["saveDone", saveJobState("error","working")],
		},
		function (err,results) {
			if (results) debug("finished %s" ,results);
			job = results.readjob;
			if (!job || typeof(job.status)== 'undefined') {
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










