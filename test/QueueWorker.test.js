var should    = require('should');
var pg        = require('pg');
var async     = require('async');
var nock      = require('nock');


var config         = require('../configuration.js');
var util           = require('../util.js');
var WorkerQueue    = require('../model/WorkerQueue.js');
var DataCollection = require('../model/DataCollection.js');
var QueueWorker    = require('../QueueWorker.js');
var wochenaufgabe  = require('../wochenaufgabe.js');
var LoadOverpassData  = require('../model/LoadOverpassData.js');
var POI  = require('../model/POI.js');

var helper = require('./helper.js');


var query =
{  DE: '[out:json][timeout:5000];area[name="Deutschland"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["de:amtlicher_gemeindeschluessel"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["de:amtlicher_gemeindeschluessel"]; \
          out;',
 AT: '[out:json][timeout:4000];area[name="Österreich"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:at:gkz"];out ids; );  \
          .pharmacies is_in; \
          area._[boundary=administrative] \
            ["ref:at:gkz"]; \
          out;',
CH: '[out:json][timeout:3600];area[name="Schweiz"]->.a;( node(area.a)[amenity=pharmacy]; \
                                                   way(area.a)[amenity=pharmacy]; \
                                                  rel(area.a)[amenity=pharmacy]; \
                                                    )->.pharmacies; \
          foreach.pharmacies(out center meta;(._; ._ >;);is_in;area._[boundary=administrative] \
          ["ref:bfs_Gemeindenummer"];out ids; ); \
          .pharmacies is_in; \
          area._[boundary=administrative] \
          out;'
}

describe('QueueWorker',function(){
  before(function(bddone){
    DataCollection.initialise(bddone);
  })
  beforeEach(function(bddone) {
    helper.initUnallowedGlobals();
    async.series([
      WorkerQueue.dropTable.bind(WorkerQueue),
      WorkerQueue.createTable.bind(WorkerQueue),
      DataCollection.dropTable.bind(DataCollection),
      DataCollection.createTable.bind(DataCollection),
      POI.dropTable.bind(POI),
      POI.createTable.bind(POI)
    ],function(err) {
      should.not.exist(err);
      bddone();
    });
  });
  afterEach(function(bddone){
    helper.checkUnallowedGlobals();
    bddone();
  })
  describe('doNextJob',function() {   
    it('should generate an Error with insert Values',function(bddone) {
      var valueList = [{id:1,measure:"notexistm",type:"insert",status:"open",exectime: new Date()}];

      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          WorkerQueue.count({status:"error"},function(err,data){
            should.not.exist(err);
            should(data).equal('1');
            bddone();
          })
        })
      });
    })
    it('should trigger insert Values and create new insert',function(bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"},overpassEveryDays:7};
      var valueList = [{id:1,measure:"test",type:"insert",status:"open",exectime: new Date()}];

      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function (done) {
              WorkerQueue.count({status:"open"},function(err,data){
                should.not.exist(err);
                should(data).equal('3');
                done();   
              })        
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should trigger insert Values without new insert',function(bddone) {
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"},overpassEveryDays:7};
      var valueList = [{id:1,measure:"test",type:"insert",status:"open",exectime: new Date()},
                       {id:2,measure:"test",type:"insert",status:"open",exectime: new Date("2999-02-02")}
      ];

      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function (done) {
              WorkerQueue.count({status:"open"},function(err,data){
                should.not.exist(err);
                should(data).equal('3');
                done();   
              })        
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work on overpass Queries',function(bddone) {
      // Adjust Timeout, as Overpass is Waiting a little bit
      this.timeout(1000*60*2+100);
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery")             
                  .replyWithFile(200, __dirname + '/LoadOverpassData.test.json');
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "done";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work on readpoi Queries',function(bddone) {
      // Adjust Timeout, as Overpass is Waiting a little bit
     // this.timeout(1000*60*2+100);

      // first prepare wochenaufgabe Map
      wochenaufgabe.map["Apotheke"].map.list=["05158008","05158007"];
      wochenaufgabe.map["Apotheke_AT"].map.list=["700"];
      var singleStep = {id:"1",schluessel:"",measure:"Apotheke",type:"readpoi",query:"",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scopeAT = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=%5Bout%3Ajson%5D%5Btimeout%3A4000%5D%3Barea%5Bname%3D%22%C3%96sterreich%22%5D-%3E.a%3B%28%20node%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20way%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20rel%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29-%3E.pharmacies%3B%20%20%20%20%20%20%20%20%20%20%20foreach.pharmacies%28out%20center%20meta%3B%28._%3B%20._%20%3E%3B%29%3Bis_in%3Barea._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20%5B%22ref%3Aat%3Agkz%22%5D%3Bout%20ids%3B%20%29%3B%20%20%20%20%20%20%20%20%20%20%20%20.pharmacies%20is_in%3B%20%20%20%20%20%20%20%20%20%20%20area._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20%20%20%5B%22ref%3Aat%3Agkz%22%5D%3B%20%20%20%20%20%20%20%20%20%20%20out%3B")
                   .reply(200,{osm3s:{timestamp_osm_base:"2015-05-26T07:44:02Z"},elements:[]});
      var scopeDE = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=%5Bout%3Ajson%5D%5Btimeout%3A5000%5D%3Barea%5Bname%3D%22Deutschland%22%5D-%3E.a%3B%28%20node%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20way%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20rel%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29-%3E.pharmacies%3B%20%20%20%20%20%20%20%20%20%20%20foreach.pharmacies%28out%20center%20meta%3B%28._%3B%20._%20%3E%3B%29%3Bis_in%3Barea._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20%5B%22de%3Aamtlicher_gemeindeschluessel%22%5D%3Bout%20ids%3B%20%29%3B%20%20%20%20%20%20%20%20%20%20%20%20.pharmacies%20is_in%3B%20%20%20%20%20%20%20%20%20%20%20area._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20%20%20%5B%22de%3Aamtlicher_gemeindeschluessel%22%5D%3B%20%20%20%20%20%20%20%20%20%20%20out%3B")
                  .replyWithFile(200, __dirname + '/POIPharmacyHaan.json');
      var scopeCH = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=%5Bout%3Ajson%5D%5Btimeout%3A3600%5D%3Barea%5Bname%3D%22Schweiz%22%5D-%3E.a%3B%28%20node%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20way%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20rel%28area.a%29%5Bamenity%3Dpharmacy%5D%3B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29-%3E.pharmacies%3B%20%20%20%20%20%20%20%20%20%20%20foreach.pharmacies%28out%20center%20meta%3B%28._%3B%20._%20%3E%3B%29%3Bis_in%3Barea._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20%5B%22ref%3Abfs_Gemeindenummer%22%5D%3Bout%20ids%3B%20%29%3B%20%20%20%20%20%20%20%20%20%20%20.pharmacies%20is_in%3B%20%20%20%20%20%20%20%20%20%20%20area._%5Bboundary%3Dadministrative%5D%20%20%20%20%20%20%20%20%20%20%20out%3B")
                   .reply(200,{osm3s:{timestamp_osm_base:"2015-05-26T07:44:02Z"},elements:[]});
      var scope1 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=286404815&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"978017","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"286404815","lat":"51.1897635","lon":"6.9995844","display_name":"Bahnhof-Apotheke, 13, Bahnhofstra\u00dfe, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Bahnhof-Apotheke","house_number":"13","road":"Bahnhofstra\u00dfe","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope2 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=291712654&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"1036425","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"291712654","lat":"51.1933829","lon":"7.0111183","display_name":"Schwanen Apotheke, 36, Neuer Markt, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Schwanen Apotheke","house_number":"36","pedestrian":"Neuer Markt","retail":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope3 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=291712655&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"1033075","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"291712655","lat":"51.1920584","lon":"7.0091031","display_name":"Adler-Apotheke, 19, Kaiserstra\u00dfe, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Adler-Apotheke","house_number":"19","road":"Kaiserstra\u00dfe","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope4 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=291712657&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"1036285","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"291712657","lat":"51.1924984","lon":"7.0120785","display_name":"Markt Apotheke, 36, Kaiserstra\u00dfe, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Markt Apotheke","house_number":"36","road":"Kaiserstra\u00dfe","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope5 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=291715366&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"1036312","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"291715366","lat":"51.1934206","lon":"7.0121372","display_name":"Bergische Apotheke, 22-24, Neuer Markt, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Bergische Apotheke","house_number":"22-24","pedestrian":"Neuer Markt","retail":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope6 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=287826898&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"997349","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"287826898","lat":"51.1943657","lon":"7.0089153","display_name":"Elefanten-Apotheke, 27-29, Neuer Markt, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Elefanten-Apotheke","house_number":"27-29","road":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      var scope7 = nock('http://open.mapquestapi.com/nominatim/v1/reverse.php')
                  .post('?format=json&osm_type=N&osm_id=89997088&zoom=18&addressdetails=1&email=OSMUser_TheFive',"")
                   .reply(200,{"place_id":"315796","licence":"Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http:\/\/www.openstreetmap.org\/copyright","osm_type":"node","osm_id":"89997088","lat":"51.2176076","lon":"7.0136172","display_name":"Gruitener Apotheke, 23, Bahnstra\u00dfe, Haan, Kreis Mettmann, Regierungsbezirk D\u00fcsseldorf, Nordrhein-Westfalen, 42781, Deutschland","address":{"pharmacy":"Gruitener Apotheke","house_number":"23","road":"Bahnstra\u00dfe","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk D\u00fcsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
      
  
      
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "done";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              POI.find({},function(err,data){
                should.not.exist(err);
                should(data.length).equal(7);
                for (var i=0;i<7;i++) {
                  delete data[i]._id;
                  delete data[i].nominatim.timestamp;
                }
                should(data).containEql({"type":"node","id":286404815,"lat":51.1897635,"lon":6.9995844,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"13","addr:postcode":"42781","addr:street":"Bahnhofstraße","amenity":"pharmacy","name":"Bahnhof-Apotheke","opening_hours":"mo-fr 08:00-18:30,sa 09:00-13:00","phone":"+49 2129 2304","website":"http://www.bahnhofapotheke-haan.de/","wheelchair":"no"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Bahnhof-Apotheke","house_number":"13","road":"Bahnhofstraße","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":291712654,"lat":51.1933829,"lon":7.0111183,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"36","addr:postcode":"42781","addr:street":"Neuer Markt","amenity":"pharmacy","dispensing":"yes","name":"Schwanen Apotheke","opening_hours":"Mo-Fr 08:00-18:30; Sa 08:00-13:30","phone":"Tel.: (02129) 59 100","website":"http://www.apotheken-dr-peterseim.de/","wheelchair":"no"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Schwanen Apotheke","house_number":"36","pedestrian":"Neuer Markt","retail":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":291712655,"lat":51.1920584,"lon":7.0091031,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"19","addr:postcode":"42781","addr:street":"Kaiserstraße","amenity":"pharmacy","dispensing":"yes","name":"Adler-Apotheke","opening_hours":"Mo-Fr 08:30-13:30,14:30-18:30; Sa 08:30-13:00","phone":"02129-9352-0","website":"http://www.apotheken-dr-peterseim.de/","wheelchair":"no"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Adler-Apotheke","house_number":"19","road":"Kaiserstraße","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":291715366,"lat":51.1934206,"lon":7.0121372,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"22-24","addr:postcode":"42781","addr:street":"Neuer Markt","amenity":"pharmacy","contact:phone":"+49 2129 93930","dispensing":"yes","name":"Bergische Apotheke","opening_hours":"Mo-Fr 08:00-19:00;Sa 08:00-13:30","phone":"+49 2129 93930","website":"www.bergische-apotheke.de/","wheelchair":"yes"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Bergische Apotheke","house_number":"22-24","pedestrian":"Neuer Markt","retail":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":287826898,"lat":51.1943657,"lon":7.0089153,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"27-29","addr:postcode":"42781","addr:street":"Neuer Markt","amenity":"pharmacy","name":"Elefanten-Apotheke","opening_hours":"Mo-Fr 08:00-18:30; Sa 09:00-13:00","phone":"Tel.: (02129) 95 96 13","website":"http://www.apotheken-dr-peterseim.de/","wheelchair":"yes"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Elefanten-Apotheke","house_number":"27-29","road":"Neuer Markt","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":89997088,"lat":51.2176076,"lon":7.0136172,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"23","addr:postcode":"42781","addr:street":"Bahnstraße","amenity":"pharmacy","name":"Gruitener Apotheke","opening_hours":"mo,tu,th,fr 08:30-13:00,15:00-18:30;we,sa 08:30-13:00","phone":"02104 60318","website":"http://www.gruitener-apotheke.de/","wheelchair":"yes"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Gruitener Apotheke","house_number":"23","road":"Bahnstraße","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                should(data).containEql({"type":"node","id":291712657,"lat":51.1924984,"lon":7.0120785,timestamp_osm_base: '2015-05-26T07:44:02Z',"tags":{"addr:city":"Haan","addr:country":"DE","addr:housenumber":"36","addr:postcode":"42781","addr:street":"Kaiserstraße","amenity":"pharmacy","dispensing":"yes","name":"Markt Apotheke","opening_hours":"mo-fr 08:00-18:30,sa 08:00-13:00","phone":"Tel.: (02129) 16 14","wheelchair":"no"},"osmArea":[{"de:regionalschluessel":"05158","de:amtlicher_gemeindeschluessel":"05158"},{"de:regionalschluessel":"05","de:amtlicher_gemeindeschluessel":"05"},{"de:regionalschluessel":"051","de:amtlicher_gemeindeschluessel":"051"},{"de:regionalschluessel":"051580008008","de:amtlicher_gemeindeschluessel":"05158008"}],"overpass":{"loadBy":"DE"},"nominatim":{"pharmacy":"Markt Apotheke","house_number":"36","road":"Kaiserstraße","town":"Haan","county":"Kreis Mettmann","state_district":"Regierungsbezirk Düsseldorf","state":"Nordrhein-Westfalen","postcode":"42781","country":"Deutschland","country_code":"de"}});
                done();
              });

            },
            function (done) {
              DataCollection.find({},function(err,data) {
                should.not.exist(err);
                should(data.length).equal(2);
                delete data[0].timestamp;
                delete data[1].timestamp;
             //   delete data[2].timestamp;

                should(data).containEql({measure:"Apotheke",schluessel:"05158008",missing:{name:'0',wheelchair:'0',phone:'0',opening_hours:'0'},existing:{fixme:'0'},source:null,count:7});
                should(data).containEql({measure:"Apotheke",schluessel:"05158007",missing:{name:'0',wheelchair:'0',phone:'0',opening_hours:'0'},existing:{fixme:'0'},source:null,count:0});
               // should(data).containEql({measure:"Apotheke_AT",schluessel:"700",missing:{name:'0',wheelchair:'0',phone:'0',opening_hours:'0'},existing:{fixme:'0'},source:null,count:0});
                done();
              })
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work handle not parsable results',function(bddone) {
      this.timeout(1000*60*2+100);
      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery")             
                  .reply(200, "Not parsable JSON");
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "error";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"error"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('0');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              bddone();
            }
          );
        })
      })
    })
    it('should work handle a timeout',function(bddone) {
      this.timeout(1000*60*2+100);
      var remindTimeout = LoadOverpassData.timeout;
      LoadOverpassData.timeout = 500;

      wochenaufgabe.map["test"]={map:{list:['1','2']},overpass:{query:"TEST :schluessel: TEST"}};
      var singleStep = {id:"1",schluessel:"102",measure:"test",type:"overpass",query:"This is an overpassquery",status:"open",exectime: new Date()};
      var valueList = [singleStep];
      var scope = nock('http://overpass-api.de/api/interpreter')
                  .post('',"data=This%20is%20an%20overpassquery") 
                  .socketDelay(2000)            
                  .replyWithFile(200, __dirname + '/LoadOverpassData.test.json');
      WorkerQueue.insertData(valueList,function(err,data) {
        should.not.exist(err);
        should(data).equal('Datensätze: 1');
        QueueWorker.doNextJob(function(err,data){
          should.not.exist(err);
          singleStep.status = "error";
          should(data).match(singleStep);
          async.parallel([
            function (done) {
              WorkerQueue.count({id:1,status:"error"},function(err,data){
                should.not.exist(err);
                should(data).equal('1');
                done(); 
              })          
            },
            function(done) {
              DataCollection.count({measure:"test",schluessel:"102",count:12},function(err,data){
                should.not.exist(err);
                should(data).equal('0');
                done();
              });
            }
            ],function(err) {
              should.not.exist(err);
              LoadOverpassData.timeout = remindTimeout;
              bddone();
            }
          );
        })
      })
    })


  })
  describe('runNextJobs',function(){
    it('should work 3 doConsole',function(bddone) {
      this.timeout(1000*60*2+100);
      var valueList = 
        [{id:1,measure:"test",type:"console",status:"open",exectime: new Date()},
         {id:2,measure:"test",type:"console",status:"open",exectime: new Date()},
         {id:3,measure:"test",type:"console",status:"open",exectime: new Date()}];
      var waiter = util.createWaiter(1);
      QueueWorker.processExit = function() {console.log('Simulated Process Exit');};
      WorkerQueue.insertData(valueList,function(err,data) {
        async.auto(
          {  runNextJobs:QueueWorker.startQueue,
             wait1sec: waiter,
             interrupt:["wait1sec",function(cb) {QueueWorker.processSignal="SIGINT";cb()}],
             test:["runNextJobs",function(cb){
                WorkerQueue.count({status:"done"},function(err,data){
                should.not.exist(err);
                should(data).equal('3');
                cb(); 
             })}]
          },function (err) {
            should.not.exist(err);
            bddone();
          }
        )
      });
    });
  })

})

