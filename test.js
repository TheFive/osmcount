var pg = require('pg');
var async = require('async');

var conString = "postgres://test:test@localhost:5432/testdb";

//var client = new pg.Client(conString);
//client.connect(function(err) {
/*
pg.connect(conString,function(err,client,pgdone) {
  if(err) {
    return console.error('could not connect to postgres', err);
  }
  client.query('SELECT * from  datacollection', function(err, result) {
    if(err) {
      return console.error('error running query', err);
    }
    console.dir(result);
    console.dir(result.rows[0]);
    //output: Tue Jan 15 2013 19:12:47 GMT-600 (CST)
    pgdone();
  });
})
*/
/*
pg.connect(conString, function(err,client,pgdone){

  client.query("select data as data from DataCollection;",function(err,data){
    console.dir(data);
    pgdone();
    });
  });
*/



  function insertData(item,callback){
    console.log("call insertData");
    console.dir(item);
    clientLocal.query("INSERT into DataCollection (data) VALUES($1)",saveData, function(err,result) {
      console.log("error");
      console.dir(err);
      console.log("result");
      console.dir(result);
      callback();
    })
}

pg.connect(conString,function(err,client,pgdone) {
  console.log("Connect Error"+err)
  function insertData(item,callback){
    client.query("INSERT into DataCollection (data) VALUES($1)",[item], function(err,result) {
      callback(err);
    })
  }

  saveData = [{name:"Walter",phone:"234"},{name:"test",phone:456}];
  for (var i =0;i<10000;i++) {
    saveData.push({name:("Name"+i),phone:10000+i})
  }

  start = (new Date()).getTime();
  async.each(saveData,insertData,function() {console.log("pgdone");pgdone();
    console.log("time"+((new Date()).getTime()-start));  
  })

})

/*
function insertData(item,callback) {
  client.query('INSERT INTO subscriptions (subscription_guid, employer_guid, employee_guid)
       values ($1,$2,$3)', [
        item.subscription_guid,
        item.employer_guid,
        item.employee_guid
       ],
  function(err,result) {
    // return any err to async.each iterator
    callback(err);
  })
}
async.each(datasetArr,insertData,function(err,result) {
  // Release the client to the pg module
  done();
  if (err) {
    set_response(500, err, res);
    logger.error('error running query', err);
    return console.error('error running query', err);
  }
  logger.info('subscription with created');
  set_response(201);
})*/
