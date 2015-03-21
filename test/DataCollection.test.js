var pg     =require('pg');
var async  = require('async');
var should = require('should');

var config = require('../configuration.js');
var DataCollection = require('../model/DataCollection.js')


describe('DataCollection', function() {
  var pgclient; 
  // Create one pgclient for all tests, and release it afterwards
  before(function(bddone) {
    config.initialisePostgresDB();
    pgclient = new pg.Client(config.postgresConnectStr);
    pgclient.connect(bddone);

  });
  after(function(bddone) {
    pgclient.end();
    bddone();
  })
  describe('aggregatePostgresDB',function() {
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

        A    2012-10-01  2012-11-01 
        101    1           
        1021              0
        1022   3          

        */
        pgclient.query("insert into datacollection (measure,key,stamp,count,source,missing,existing)  \
                         values ('test','101',to_date('2012-10-01','YYYY-MM-DD'),10,'1','A=>1','fixme=>101'),\
                                ('test','1021',to_date('2012-10-01','YYYY-MM-DD'),12,'1','B=>1','fixme=>1021'),\
                                ('test','1022',to_date('2012-10-01','YYYY-MM-DD'),23,'1','A=>3','fixme=>1022'),\
                                ('test','101',to_date('2012-11-01','YYYY-MM-DD'),11,'2','B=>1','fixme=>1'),\
                                ('test','1021',to_date('2012-11-01','YYYY-MM-DD'),13,'2','A=>0','fixme=>2'),\
                                ('test','1022',to_date('2012-11-01','YYYY-MM-DD'),24,'2','B=>1','fixme=>3');",
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
      it('should group 2 timeline with last Values', function(bddone) {
        param = {
              lengthOfKey:4,
              lengthOfTime:4,
              measure:'test',
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should(data).match([ { _id: { row: '101', col: '2012' }, cell: 11 },
                               { _id: { row: '1022', col: '2012' }, cell: 24 },
                               { _id: { row: '1021', col: '2012' }, cell: 13 } ]);
          bddone();
        })
      })
      it('should group 2 timeline with last Values with percentage calculation', function(bddone) {
        param = {
              lengthOfKey:4,
              lengthOfTime:4,
              measure:'test',
              sub : "missing.B",
              subPercent : "Yes"
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should(data).match([ { _id: { row: '101', col: '2012' }, cell: 0.0909090909090909 },
                               { _id: { row: '1022', col: '2012' }, cell: 0.0416666666666667 },
                               { _id: { row: '1021', col: '2012' }, cell: 0 } ]);
          bddone();
        })
      })
      it('should sum up missing Values', function(bddone) {
        param = {
              lengthOfKey:4,
              lengthOfTime:7,
              measure:'test',
              sub:"missing.A"
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should(data).containEql({ _id: { row: '101', col: '2012-10' }, cell: 1 });
          should(data).containEql({ _id: { row: '1021', col: '2012-10' }, cell: 0 });
          should(data).containEql({ _id: { row: '1022', col: '2012-10' }, cell: 3 });
          should(data).containEql({ _id: { row: '101', col: '2012-11' }, cell: 0 });
          should(data).containEql({ _id: { row: '1021', col: '2012-11' }, cell: 0 });
          should(data).containEql({ _id: { row: '1022', col: '2012-11' }, cell: 0 });
          bddone();
        })
      })
      it('should sum up existing Values', function(bddone) {
        param = {
              lengthOfKey:2,
              lengthOfTime:7,
              measure:'test',
              sub:"existing.fixme"
        };
        DataCollection.aggregate(param,function done(err,data) {
          should.equal(err,null);
          should(data).containEql({ _id: { row: '10', col: '2012-10' }, cell: 2144 });
          should(data).containEql({ _id: { row: '10', col: '2012-11' }, cell: 6 });
          bddone();
        })
      })
    });
  })
  describe('count', function() {
    before( function(bddone){
       async.series([
        DataCollection.dropTable,
        DataCollection.createTable
      ],function(err) {
        if (err) console.dir(err);
        should.not.exist(err);
        

       /* Short Data Table
             2012-10-01  2012-11-01 
        101    10          11
        1021   12          13
        1022   23          24

        A    2012-10-01  2012-11-01 
        101    1           
        1021              0
        1022   3          

        */
      pgclient.query("insert into datacollection (measure,key,stamp,count,source,missing,existing)  \
                         values ('testa','101',to_date('2012-10-01','YYYY-MM-DD'),10,'1','A=>1','fixme=>101'),\
                                ('testa','1021',to_date('2012-10-01','YYYY-MM-DD'),12,'1','B=>1','fixme=>1021'),\
                                ('testa','1022',to_date('2012-10-01','YYYY-MM-DD'),23,'1','A=>3','fixme=>1022'),\
                                ('testb','101',to_date('2012-11-01','YYYY-MM-DD'),11,'2','B=>1','fixme=>1'),\
                                ('testb','1021',to_date('2012-11-01','YYYY-MM-DD'),13,'2','A=>0','fixme=>2'),\
                                ('testb','1022',to_date('2012-11-01','YYYY-MM-DD'),24,'2','B=>1','fixme=>3');",
        bddone);
      });
    });
    it('should count measure', function (bddone) {
      DataCollection.count({measure:'testa'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(3);
        bddone();
      });
    })
    it('should count key (no regex)', function (bddone) {
      DataCollection.count({schluessel:'1021'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(2);
        bddone();
      })
    })
    it('should count key (regex)', function (bddone) {
      DataCollection.count({schluessel:'^102'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(4);
        bddone();
      })
    })
    it('should count source', function (bddone) {
      DataCollection.count({source:1},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(3);
        bddone();
      })
    })
    it('should count timestamp', function (bddone) {
      DataCollection.count({timestamp:'2012-10-01'},function(err,data){
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(3);
        bddone();
      })
    })
    it('should count count', function (bddone) {
      DataCollection.count({count:13},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(1);
        bddone();
      });
    })
    it('should count multiple Values', function (bddone) {
      DataCollection.count({source:1,measure:'testa',schluessel:'^102'},function(err,data) {
        should.not.exist(err);
        data = parseInt(data);
        should(data).equal(2);
        bddone();
      });
    })
  })
})
