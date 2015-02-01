var config    = require('./configuration.js');
var lod       = require('./LoadOverpassData.js');
var util      = require('./util.js')
var async     = require('async');
var debug     = require('debug')('QueueWorker');
 debug.entry  = require('debug')('QueueWorker:entry');
 debug.data   = require('debug')('QueueWorker:data');
var ObjectID  = require('mongodb').ObjectID;


function getWorkingJob(cb) {	
	debug.entry("getWorkingJob(cb)");
	mongodb = config.getDB();
	mongodb.collection("WorkerQueue").findOne( 
							{ status : "working" 
							}, function(err, obj) 
	{
		debug.entry("getWorkingJob->CB("+err+","+obj+")");
		if (err) {
			console.log("Error occured in function: QueueWorker.getNextJob");
			console.log(err);
		}

		if (obj) {
			debug.data("found job %s",obj.type);		
			// return obj as job object
			obj.status="open";
		}
		cb(err,obj);
	}
)}


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
			result.source = job.source;
			async.series( [
				function (cb) {
					lod.runOverpass(query,job,result,cb);
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

function doOverpassPOIPLZ(cb,results) {
	debug.entry("doOverpassPOIPLZ(cb,"+results+")");
	job=results.readjob;
	debug.data(JSON.stringify(job));
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpassPOIPLZ") {
		debug.entry("Start: doOverpassPOIPLZ(cb,"+results+")");
			plz=job.plz;
			query=job.query;
			var result= {};
			result.plz = job.plz;
			result.source = job.source;
			async.series( [
				function (cb) {
				///
				///
				/// TBD Here: runOverpass Austauschen in Abfrage
				/// und anschliessenden Anstossen von 
				/// nominatim
				////
					lod.runOverpass(query,job,result,cb);
				},
				function (cb) {
					saveMeasure(result,cb);
					
				}],
				function (err,ergebnis) {
					debug.entry("doOverpassPOIPLZ->finalCB	");
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
		jobs = lod.createQuery(job.measure,job.exectime,job);
		console.log("Trigger to load "+job.measure+" at "+job.exectime + " Number Jobs "+ jobs.length);
		if (jobs.length == 0) {
			// No Jobs created
			console.log("Nothing loaded");
			job.status = "error";
			job.error = "createQuery results in 0 Jobs";
			if (cb) cb(null,job);
			return;
		}
		mongodb.collection("WorkerQueue").insert(jobs,
			function (err, records) {
				if (err) {
					console.log("Error occured in function: QueueWorker.doInsertJobs");
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

function doUpdatePOI(cb,results) {
	debug.entry("doUpdatePOI(cb,"+results+")");
	job=results.readjob;
	
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="updatePOI") {
		debug.entry("Start: doInsertJobs(cb,"+results+")");
		mongodb = config.getDB();
		source = job.source;
		
        configuration.getDB().collection("DataCollection").find( 
							      {source:source}).each(
								function(err,result) {
          if (err) {
            console.log("Error: "+JSON.stringify(err));
            cb(err,null);
            return;
          }
          if (result == null) {
        	cb();
          }
        
  }


								
});}
	
	}
	else if (cb) cb(null,job);
}

function doLoadBoundaries(cb,results) {
	debug.entry("doLoadBoundaries(cb,"+results+")");
	job=results.readjob;
	
	if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="loadBoundaries") {
		debug.entry("Start: doLoadBoudnaries(cb,"+results+")");
		lod.importBoundaries(job,cb);
	}
	else if (cb) cb(null,job);
}
function checkState(cb,results) {
	debug.entry("checkState(cb,"+results+")");
	job=results.readjob;
	if (!job) {
		debug.data("No Job");
		cb(null,null);
		return;
	}
	mongodb = config.getDB();
	mongodb.collection("DataCollection").count( 
							{ source : job.source,schluessel:job.schluessel}
							 
							, function(err, count) 
	{
		debug.entry("checkState->CB");
		if (count==1) {
			job.status = "done";
			debug.data("Close Job");
		}
		else if (count==0) {
			job.status = "open";
			debug.data("Job Still open");
		} else {
			job.status = "error";
			job.error = "Found "+count+" matching this request " +job.source+ " " + job.schluessel;
		}
		if (cb) cb(null,count);

	})
}




function doNextJob(callback) {
	debug.entry("doNextJob(cb)");
	async.auto( {readjob:     getNextJob,
		    saveWorking:     ["readjob",saveJobState],
		    doConsole:       ["saveWorking", doConsole],
		    doOverpass:      ["saveWorking", doOverpass],
		    doInsertJobs:    ["saveWorking", doInsertJobs],
		    doLoadBoudnaries:["saveWorking", doLoadBoundaries],
		    saveDone:        ["doConsole","doOverpass","doInsertJobs", saveJobState]
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




function correctData(callback) {
	debug.entry("correctData(cb)");
	async.auto( {readjob:     getWorkingJob,
		         correctData: ["readjob",checkState],
		   		 saveDone:    ["correctData", saveJobState]
		},
		function (err,results) {
			if (err) {
				console.log("Error occured in function: QueueWorker.doNextJob");
				console.log(err);
			}
			if (results) debug("finished %s" ,results);
			job = results.readjob;
			if (job ) {
				q.push(correctData);
			}
			callback();
		}
	)
}

exports.startQueue =function(cb) {
	debug.entry("startQueue(cb)");
	q.push(config.initialiseDB);
	q.push(correctData);
	q.push(doNextJob);
	if (cb) cb();
}






