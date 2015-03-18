var pg=require('pg');
var async = require('async');
var should = require('should');

var configuration = require('../configuration.js');
var DataCollection = require('../model/DataCollection.js')


describe('DataCollection', function() {
  describe('aggregatePostgresDB',function() {
    configuration.initialisePostgresDB();

    // Create one pgclient for all tests, and release it afterwards
    var pgclient;
    before(function(bddone) {
      pgclient = new pg.Client(configuration.postgresConnectStr);
      pgclient.connect(bddone);

    });
    after(function(bddone) {
      pgclient.end();
      bddone();
    })


    beforeEach(function(bddone) {
      async.series([
        DataCollection.dropTable,
        DataCollection.createTable
      ],function(err) {
        if (err) console.dir(err);
        should.not.exist(err);
        bddone();
      });
    });
    context('test different group functions',function(bddone){
      beforeEach( function(bddone){
        /* Short Data Table
             2012-10-01  2012-11-01
        101    10          11
        1021   12          13
        1022   23          24
        */
        pgclient.query("insert into datacollection (measure,key,stamp,count,source)  \
                         values ('test','101',to_date('2012-10-01','YYYY-MM-DD'),10,'1'),\
                                ('test','1021',to_date('2012-10-01','YYYY-MM-DD'),12,'1'),\
                                ('test','1022',to_date('2012-10-01','YYYY-MM-DD'),23,'1'),\
                                ('test','101',to_date('2012-11-01','YYYY-MM-DD'),11,'2'),\
                                ('test','1021',to_date('2012-11-01','YYYY-MM-DD'),13,'2'),\
                                ('test','1022',to_date('2012-11-01','YYYY-MM-DD'),24,'2');",
          bddone);

      });
      it ('should group 2 keys', function(bddone) {
        param = {
              lengthOfKey:2,
              lengthOfTime:10,
              measure:'test',
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should.equal(data.length,2);
          should.equal(data[0].cell,45);
          should.equal(data[0]._id.col,'2012-10-01');
          should.equal(data[0]._id.row,'10');
          should.equal(data[1].cell,48);
          should.equal(data[1]._id.col,'2012-11-01');
          should.equal(data[1]._id.row,'10');
          bddone();
        })
      })
 /*     it ('should group 2 timeline with last Values', function(bddone) {
        param = {
              lengthOfKey:4,
              lengthOfTime:4,
              measure:'test',
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should.equal(data.length,3);
          should.equal(data[0].cell,11);
          should.equal(data[0]._id.col,'2012');
          should.equal(data[0]._id.row,'10');
          should.equal(data[1].cell,13);
          should.equal(data[1]._id.col,'2012');
          should.equal(data[1]._id.row,'1021');
          should.equal(data[2].cell,24);
          should.equal(data[2]._id.col,'2012');
          should.equal(data[2]._id.row,'1022');
          bddone();
        })
      })*/
    });
  })
})
