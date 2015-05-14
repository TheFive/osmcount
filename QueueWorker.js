
var debug     = require('debug')('QueueWorker');
var async     = require('async');
var fs        = require('fs');
var should    = require('should');


var config      = require('./configuration.js');
var lod         = require('./model/LoadOverpassData.js');
var WorkerQueue = require('./model/WorkerQueue.js');
var util        = require('./util.js')

var DataCollection = require('./model/DataCollection.js')
var wochenaufgabe = require('./wochenaufgabe.js');

exports.processSignal = '';
exports.processExit;

var overpassWaitTime = 3500; // Wait before another Overpass Query
var overpassWaitTimeSteps = 50;
var overpassNo429erFor = 0;

exports.overpassWaitTimeInfo = function() 
{ return "[OWT:"+overpassWaitTime+",no429:"+overpassNo429erFor+"]";}

function getHangingJob(cb) {
  debug("getHangingJob(cb)");

  // First check to find an working Job for Correction
  WorkerQueue.getWorkingTask(function(err, obj)
  {
    debug("getHangingJob->CB("+err+","+obj+")");
    if (err) {
      console.log("Error occured in function: QueueWorker.getHangingJob");
      console.log(err);
    }

    if (obj) {
      debug("found job %s",obj.type);
      // return obj as job object
      obj.status="open";
    } else {
      var err = "No Working Job Found";
      cb(err,null);
      return;
    }
    cb(err,obj);
  }
)}


function getNextJob(cb) {
  debug("getNextJob(cb)");
 
  WorkerQueue.getNextOpenTask(function(err, obj)
  {
    debug("getNextJob->CB("+err+","+obj+")");
    if (err) {
      console.log("Error occured in function: QueueWorker.getNextJob");
      console.log(err);
    }

    if (obj) {
      debug("found job %s",obj.type);
      // return obj as job object
      obj.status="working";
    }
    else {
      // No Job loaded
      var err = {}
      err.message = "No Job Loaded";
      cb(err,null);
      return;
    }
    cb(err,obj);
  }
)}


function saveMeasure(result,cb) {
  debug("saveMeasure("+result+",cb)");
  should.exist(result);
  DataCollection.save(result, function (err, num){
    debug("saveMeasure->CB("+err+","+num+")");
    if (err) {
      console.log("Error occured in function: OueueWorker.saveMeasure");
      console.log("Tried to save:");
      console.log(err);
      err.func="saveMeasure";
      cb(err)
      return;
    }
    cb(null);
    return;
  }
)}

function saveJobState(cb,result) {
  debug("saveJobState Function(cb,"+job+")");
  var job = result.readjob;
  should.exist(job);
  should.exist(job.status);
  var date = new Date();
  job.timestamp = date;
  debug("Saving Jobsstatus to %s",job.status);
  debug("saveJobState()->call CB");
  WorkerQueue.saveTask(job, function (err, num){
    debug("saveJobState()->CB("+err+","+num+")");
    if (err) {
      console.log("Error occured in function: OueueWorker.saveJobState");
      console.log(err);
    } else {

    }
    cb(err,job);
  })
}


// Creating async queue and start to initialise Database
// (should be done in server in future)
var q= async.queue(function (task,cb) {
  debug("async.queue Start Next Task");
  task(cb);
},1);

function doConsole(cb,results) {
  debug("doConsole(cb,"+results+")");
  var job=results.readjob;
  if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type == "console") {
    debug("Start: doConsole(cb,"+results+")");
    debug(job.text);
    job.status = "done";
  }
  cb(null,job);
}

exports.overpassRunning = false;
exports.overpassStartTime= 0;
exports.overpassLocation= "Not Defined";
exports.overpassMeasure = "Not Defined";

function doOverpass(cb,results) {
  debug("doOverpass(cb,"+results+")");
  var job=results.readjob;
  var startTime = new Date().getTime();
  var startSave;
  var endSave;
  //debug(JSON.stringify(job));
  if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpass") {
    debug("Start: doOverpass(cb,"+results+")");
      measure=job.measure;
      query=job.query;
      var result= {};
      result.schluessel = job.schluessel;
      result.source = job.source;
      async.series( [
        function (cb) {setTimeout(cb,overpassWaitTime);},
        function (cb) {
          should(exports.overpassRunning).isFalse;
          exports.overpassRunning = true;
          exports.overpassMeasure = measure;
          exports.overpassLocation = job.schluessel;
          exports.overpassStartTime =new Date().getTime();
          lod.runOverpass(query,job,result,cb);
        },
        function (cb) {
          exports.overpassRunning = false;
          startSave = new Date().getTime();
          saveMeasure(result,cb);

        }],
        function (err,ergebnis) {
          endSave = new Date().getTime();
          debug("doOverpass->finalCB  ");
          if (err) {
            console.log("Error occured in function: QueueWorker.doOverpass");
            console.log(err);
            job.status = "error";
            job.error = err;
            // error is handled, so put null as error to save
            if (err.statusCode = "429") {
              overpassWaitTime += overpassWaitTimeSteps;
              overpassNo429erFor = 0;
              job.status = "open";
              job.error.fix = "Was HTTP 429 Fixed automated for reason NotExcecuted, actual overpass slow down: "+overpassWaitTime+"ms";
            }
          } else {
            job.status="done";
            overpassNo429erFor += 1;
            if (overpassNo429erFor >100) {
               overpassWaitTime -= overpassWaitTimeSteps;
               if (overpassWaitTime <=0)  overpassWaitTime = 0;
               overpassNo429erFor = 0;
            }
          }
          var time1 = exports.overpassStartTime - startTime;
          var time2 = startSave - exports.overpassStartTime;
          var time3 = endSave - startSave;
          var cvsLine =   '"'+job.exectime+'",'
                         +'"'+job.timestamp+'",'
                         +'"'+job.schluessel+'",'
                         + time1+  ','
                         + time2 + ','
                         + time3 + ','
                         + job.overpassTime + '\n';

          fs.appendFile('time.log', cvsLine, function (err) {
              if (err) {
               console.log("Error writing time log "+job.schluessel + " "+JSON.stringify(err));
            }

                    });

          cb(null,job);
        }

      )
  } else cb(null,job);
}

function doOverpassPOIPLZ(cb,results) {
  debug("doOverpassPOIPLZ(cb,"+results+")");
  job=results.readjob;
  //debug(JSON.stringify(job));
  if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="overpassPOIPLZ") {
    debug("Start: doOverpassPOIPLZ(cb,"+results+")");
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
          debug("doOverpassPOIPLZ->finalCB  ");
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
  debug("doInsertJobs(cb,"+results+")");
  var job=results.readjob;
  should.exist(job);
  should.exist(job.status);
  if (job.status =="working" && job.type=="insert") {
    debug("Start: doInsertJobs(cb,"+results+")");
    var jobs = lod.createQuery(job);
    
    console.log("Trigger to load "+job.measure+" at "+job.exectime + " Number Jobs "+ jobs.length);
    if (jobs.length == 0) {
      // No Jobs created
      console.log("Nothing loaded");
      job.status = "error";
      job.error = "createQuery results in 0 Jobs";
      if (cb) cb(null,job);
      return;
    }
    WorkerQueue.count({measure:job.measure,status:"open",type:"insert"},function(err,count){
      if (count && count =="0") {
        var extend = wochenaufgabe.map[job.measure].overpassEveryDays;
        if (typeof extend != 'undefined') {
          var time = job.exectime.getTime();
          var newExectime = new Date();
          time += (1000 * 60 * 60 * 24)*extend;
          newExectime.setTime(time);

          var newJob = {measure:job.measure,status:"open",type:"insert",exectime:newExectime};
          jobs.push(newJob);

        }
      }   
      job.status = "done";
      WorkerQueue.insertData(jobs,cb);
    })
  }
  else if (cb) cb(null,job);
}

function doLoadBoundaries(cb,results) {
  debug("doLoadBoundaries(cb,"+results+")");
  job=results.readjob;

  if (job && typeof(job.status)!='undefined' && job.status =="working" && job.type=="loadBoundaries") {
    debug("Start: doLoadBoudnaries(cb,"+results+")");
    console.log("Loading Boundaries");
    lod.importBoundaries(job,cb);
  }
  else if (cb) cb(null,job);
}

function checkState(cb,results) {
  debug("checkState(cb,"+results+")");
  var job=results.readjob;
  if (!job) {
    debug("No Job");
    cb(null,null);
    return;
  }
  if (job.type != "overpass") {
    debug("No Overpass Job");
    cb(null,null);
    return;
  }
  DataCollection.count(
              { source : job.source,schluessel:job.schluessel}

              , function(err, count)
  {
    debug("checkState->CB");
    if (count==1) {
      job.status = "done";
      debug("Close Job");
    }
    else if (count==0) {
      job.status = "open";
      debug("Job Still open");
    } else {
      job.status = "error";
      job.error = "Found "+count+" matching this request " +job.source+ " " + job.schluessel;
    }
    if (cb) cb(null,count);

  })
}




function doNextJob(callback) {
  debug("doNextJob(cb)");
  async.auto( {readjob:     getNextJob,
        saveWorking:     ["readjob",saveJobState],
        doConsole:       ["saveWorking", doConsole],
        doOverpass:      ["saveWorking", doOverpass],
        doInsertJobs:    ["saveWorking", doInsertJobs],
        doLoadBoudnaries:["saveWorking", doLoadBoundaries],
        saveDone:        ["doConsole","doOverpass","doInsertJobs", "doLoadBoudnaries",saveJobState]
    },
    function (err,results) {
      if (err && !(typeof(err.message) != "undefined" && err.message === 'No Job Loaded' )) {

        console.log("Error occured in function: QueueWorker.doNextJob");
        console.log(err);
      }
      if (results) debug("finished %s" ,results);
      var job = results.readjob;
      callback(null,job);
    }
  )
}

function runNextJobs(callback) {
  debug('runNextJobs');
  async.auto({
    doNextJob: doNextJob
  },
    function runNextJobsCB(err,results) {
      debug('runNextJobsCB');
      if (err) {
        console.log("Error occured in function: QueueWorker.doNextJob");
        console.log(err);
      }
     if (exports.processSignal=='SIGINT') {
        console.log( "\nExiting OSMCount" );
        exports.processExit();
      } else {
        var job = results.doNextJob;
        if (!job || typeof(job.status)== 'undefined') {
          q.push(util.waitOneMin);
        }
        q.push(runNextJobs);
      }
      debug('leaving runNextJobs');
      callback();        
    })
}

exports.doNextJob = doNextJob;
exports.runNextJobs = runNextJobs;


function correctData(callback) {
  debug("correctData(cb)");
  async.auto( {readjob:     getHangingJob,
             correctData: ["readjob",checkState],
            saveDone:    ["correctData", saveJobState]
    },
    function (err,results) {
      if (err) {
        if (err != "No Working Job Found") {
          console.log("Error occured in function: QueueWorker.correctData");
          console.log(err);
        }
      }
      if (results) debug("finished %s" ,results);
      var job = results.readjob;
      if (job ) {
        q.push(correctData);
      }
      callback();
    }
  )
}

exports.startQueue =function(cb) {
  debug("startQueue(cb)");
  q.drain = function () {cb();}
  q.push(config.initialiseMongoDB);
  q.push(correctData);
  q.push(runNextJobs);
}
