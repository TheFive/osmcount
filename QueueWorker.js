var config = require('./configuration.js');
var lod = require('./LoadOverpassData.js');
var util = require('./util.js')
var async=require('async');
var debug        = require('debug')('QueueWorker');
 debug.entry   = require('debug')('QueueWorker:entry');
 debug.data    = require('debug')('QueueWorker:data');





function getNextJob(cb) {	
	debug.entry("getNextJob(cb)");
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
		if (err) {
			console.log("Error occured in function: QueueWorker.getNextJob");
			console.log(err);
		}

		if (obj) {
			debug.data("found job %s",obj.type);		
			// return obj as job object
			obj.status="working";
		}
		cb(err,obj);
	}
)}

function saveMeasure(result,cb) {
	debug.entry("saveMeasure("+result+",cb)");

	if (result==null) {
		cb();
		return;
	}
	debug.entry("saveMeasure->call CB");
	mongodb.collection("DataCollection").save(result,{w:1}, function (err, num){
		debug.entry("saveMeasure->CB("+err+","+num+")");
	    if (err) {
	    	console.log("Error occured in function: OueueWorker.saveMeasure");
	    	console.log("Tried to save:");
	    	console.dir(result);
	     	console.log(err);
	     	cb()
	     	return;
		}
		cb();
		return;
	}	
)}

function saveJobState(cb,job) {
			debug.entry("saveJobState Function(cb,"+job+")");
			job = job.readjob;
			if (job) debug.data("job.status=%s",job.status);
			if (job==null || job.status == 'undefined') {
				debug.data("saveJobState(): Jobstatus unclear: Nothing to execute");
				cb(null,job);
				return;
			}
			date = new Date();
			job.timestamp = date;
			debug("Saving Jobsstatus to %s",job.status);
			debug.entry("saveJobState()->call CB");
			mongodb.collection("WorkerQueue").save(job,{w:1}, function (err, num){
				debug.entry("saveJobState()->CB("+err+","+num+")");
				if (err) {
					console.log("Error occured in function: OueueWorker.saveJobState");
					console.log(err);
				} else {
					
				}
				cb(err,job);
			}	
	)}


// Creating async queue and start to initialise Database
// (should be done in server in future)
var q= async.queue(function (task,cb) {
	debug.entry("async.queue Start Next Task");
	task(cb);
},1);

function doConsole(cb,results) {
	debug.entry("doConsole(cb,"+results+")");
	job=results.readjob;
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type == "console") {
		debug.entry("Start: doConsole(cb,"+results+")");
		debug(job.text);
		console.log(job.text),
		job.status = "done";
	}  
	cb(null,job);	
}


function doOverpass(cb,results) {
	debug.entry("doOverpass(cb,"+results+")");
	job=results.readjob;
	debug(JSON.stringify(job));
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpass") {
		debug.entry("Start: doOverpass(cb,"+results+")");
			measure=job.measure;
			query=job.query;
			var result= {};
			result.schluessel = job.schluessel;
			async.series( [
				function (cb) {
					lod.runOverpass(query,measure,result,cb);
				},
				function (cb) {
					saveMeasure(result,cb);
					
				}],
				function (err,ergebnis) {
					debug.entry("doOverpass->finalCB	");
					if (err) {
						console.log("Error occured in function: QueueWorker.doOverpass");
						console.log(err);
						job.status = "error";
						job.error = err;
						// error is handled, so put null as error to save
					} else {
						job.status="done";
					}
					cb(null,job);
				}
				
			)		
	} else cb(null,job);
}



function doInsertJobs(cb,results) {
	debug.entry("doInsertJobs(cb,"+results+")");
	job=results.readjob;
	
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="insert") {
		debug.entry("Start: doInsertJobs(cb,"+results+")");
		mongodb = config.getDB();
		jobs = lod.createQuery("AddrWOStreet",job.timestamp);
		console.log("Trigger to load AddrWOStreet at "+job.timestamp);
		mongodb.collection("WorkerQueue").insert(jobs,
			function (err, records) {
				if (err) {
					console.log("Error occured in function: QueueWorker.doInserJobs");
					console.log(err);
					job.status="error";
					job.error = err;
					err=null; //error wird gespeichert, kann daher hier auf NULL gesetzt werden.
				} else {
					debug("All Inserted %i" ,records.length);
					job.status='done';
				}					
				cb(err,job);
			})
	}
	else if (cb) cb(null,job);
}




function doNextJob(callback) {
	debug.entry("doNextJob(cb)");
	async.auto( {readjob:     getNextJob,
		    saveWorking: ["readjob",saveJobState],
		    doConsole:   ["saveWorking", doConsole],
		    doOverpass:  ["saveWorking", doOverpass],
		    doInsertJobs:["saveWorking", doInsertJobs],
		    saveDone:    ["doConsole","doOverpass","doInsertJobs", saveJobState]
		},
		function (err,results) {
			if (err) {
				console.log("Error occured in function: QueueWorker.doNextJob");
				console.log(err);
			}
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
	debug.entry("startQueue(cb)");
	q.push(config.initialiseDB);
	q.push(doNextJob);
	if (cb) cb();
}










