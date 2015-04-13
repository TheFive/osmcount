var pg     = require('pg');
var fs     = require('fs');
var path   = require('path');
var async  = require('async');
var should = require('should');

var config      = require('../configuration.js');
var WorkerQueue = require('../model/WorkerQueue.js')


describe('WorkerQueue', function() {
  before(function(bddone) {
    config.initialisePostgresDB();
    pgclient = new pg.Client(config.postgresConnectStr);
    pgclient.connect(bddone);

  });
  after(function(bddone) {
    pgclient.end();
    bddone();
  })
  describe('import',function(bddone) {
    beforeEach(function(bddone) {
      async.series([
        WorkerQueue.dropTable.bind(WorkerQueue),
        WorkerQueue.createTable.bind(WorkerQueue)
      ],function(err) {
        if (err) console.dir(err);
        should.equal(null,err);
        bddone();
      });
    });
    it('should import data',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.json");
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      WorkerQueue.import(filename,function(err,data){
        if (err) console.dir(err);
        should.equal(err,null);
        should(data).eql("Datensätze: 3");
        bddone();
      });
    });
    it('should handle wrong connection',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.json");
      var configTemp = config.postgresConnectStr;
      config.postgresConnectStr = "illegalConnectionString";
      WorkerQueue.import(filename,function(err,data){
        should.exist(err);
        config.postgresConnectStr = configTemp;
        bddone();
      });
    });
    it('should handle wrong filenames',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.jsonNEXIST");
      WorkerQueue.import(filename,function(err,data){
        should.exist(err);
        bddone();
      });
    });
  });
  describe('count',function(bddone){
    before( function(bddone){
       async.series([
        WorkerQueue.dropTable.bind(WorkerQueue),
        WorkerQueue.createTable.bind(WorkerQueue)
      ],function(err) {
        if (err) console.dir(err);
        should.not.exist(err);

      pgclient.query("insert into workerqueue (measure,key,status,type,source,stamp,exectime)  \
                         values ('testa','101' ,'done','overpass','AA',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD')),\
                                ('testa','1012','done','overpass','AA',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD')),\
                                ('testa','1013','done','overpass','AA',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD')),\
                                ('testb','101' ,'working','overpass','AA',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD')),\
                                ('testb','1012','open','overpass','BB',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD')),\
                                ('testb','1013','open','overpass','BB',to_date('2012-10-01','YYYY-MM-DD'),to_date('2012-10-01','YYYY-MM-DD'))",
        bddone);
      });
    });
    it('should count all', function (bddone) {
      WorkerQueue.count({},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(6);
        bddone();
      });
    })
    it('should count measure', function (bddone) {
      WorkerQueue.count({measure:'testa'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(3);
        bddone();
      });
    })
    it('should count key (no regex)', function (bddone) {
      WorkerQueue.count({schluessel:'1021'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(0);
        bddone();
      })
    })
    it('should count key (regex)', function (bddone) {
      WorkerQueue.count({schluessel:'^101'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(6);
        bddone();
      })
    })
    it('should count source', function (bddone) {
      WorkerQueue.count({source:'BB'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(2);
        bddone();
      })
    })
    it('should count timestamp', function (bddone) {
      WorkerQueue.count({timestamp:'2012-10-01'},function(err,data){
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(6);
        bddone();
      })
    })
    it('should count status', function (bddone) {
      WorkerQueue.count({status:'open'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(2);
        bddone();
      });
    })
    it('should count type', function (bddone) {
      WorkerQueue.count({type:'overpass'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(6);
        bddone();
      });
    })
    it('should count multiple Values', function (bddone) {
      WorkerQueue.count({measure:'testb',status:'working'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(1);
        bddone();
      });
    })
    it('should handle an error', function (bddone) {
      var conStr = config.postgresConnectStr;
      config.postgresConnectStr = "no connection with this string";
      WorkerQueue.count({measure:'testb',status:'working'},function(err,data) {
        should.exist(err);
        config.postgresConnectStr = conStr;
        bddone();
      });
    })
  })
  describe('saveTask',function(bddone) {
    beforeEach(function(bddone) {
      async.series([
        WorkerQueue.dropTable.bind(WorkerQueue),
        WorkerQueue.createTable.bind(WorkerQueue)
      ],function(err) {
        if (err) console.dir(err);
        should.equal(null,err);
        bddone();
      });
    });
    it('should getNextTask and Update it',function(bddone){
      var filename = path.resolve(__dirname, "WorkerQueue.test.json");
      //var filestring = fs.readFileSync(filename,{encoding:'UTF8'});
      WorkerQueue.import(filename,function(err,data){
        should.not.exist(err);
        WorkerQueue.getNextOpenTask(function(err,data) {
          should.not.exist(err);
          data.status = "working";
          var key = data.schluessel;
          WorkerQueue.saveTask(data,function(err,data){
            should.not.exist(err);
            WorkerQueue.getWorkingTask(function(err,data) {
              should.not.exist(err);
              should.exist(data);
              should(data.schluessel).equal(key);
              bddone();
            })
          })
        });
      });
    });
  });
})
