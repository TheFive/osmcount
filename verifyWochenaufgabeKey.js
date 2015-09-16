var lod = require('./model/LoadOverpassData.js');
var config = require('./configuration.js');
var async = require('async');
var fs = require('fs');
config.initialise();

var wd= require('./wochenaufgabe_data.js');

var file = fs.readFileSync("wd_data.json","utf8");


var options = {overpassUrl:'http://dev.overpass-api.de/api_mmd_test_only/interpreter'};
var actual = 0;
var allKeysCount = Object.keys(wd.dachKeyList).length;
async.eachSeries(wd.dachKeyList,function(item,cb) {
  actual +=1;
  if (typeof(item.osmkey) == 'undefined') return cb();
  if (item.lastCheck) {
    var today = new Date();
    if ((today.getTime() - new Date(item.lastCheck).getTime())<1000*60*60*24*10) {
      return cb();
    }
  }
  //console.log("Testing: "+item.name);
  var overpass = "[out:json][timeout:900];rel['"+item.osmkey+"'='"+item.osmvalue+"'][boundary=administrative];out tags;";
  //console.log(overpass);
  lod.overpassQuery(overpass,function(err,result) {
    //console.log(result);
    var r2 = JSON.parse(result)
    if (r2.elements.length==0) {
      console.log("Error for "+item.name+" "+item.osmkey+" "+item.osmvalue);
      var overpass2 = "[out:json][timeout:900];rel['"+item.osmkey+"'~'^"+item.osmvalue+"'][boundary=administrative];out;"
      console.log("Error "+item.osmvalue+" "+item.osmkey+" "+ item.name);
     // console.log("SEARCH"+overpass2);
      lod.overpassQuery(overpass2,function(err,result2){
        if (result2) {
         // console.dir(result2);
         var r = JSON.parse(result2);
         if (r.elements.length == 0 ){
          console.log("Found nothing alternativ. Check key and Value");
         }
         if (r.elements.length == 1) {
          console.log(item.osmvalue+" -> "+r.elements[0].tags[item.osmkey]); 
          item.osmvalue = r.elements[0].tags[item.osmkey];   
          item.lastCheck = new Date();      
         }
         else {
          console.log("Found multiple codes");
          console.log("Used: "+overpass2);
          for (var z=0;z<r.elements.length;z++) {
            console.log(item.osmkey+ " "+r.elements[z][item.osmkey]);
          } 

         }
         
         cb();
 
        } else console.log("No Solution");
      },options)
    } else {
      //console.log("Key Esistiert"+item.osmkey+" "+item.osmvalue);
      //console.log(overpass);
      //console.dir(result);

      console.dir(Math.round(1.0*actual/allKeysCount)*100+"% "+item.name);
      item.lastCheck = new Date();      

      cb();
    }
  },options);
},function(err){
  if (err) console.log(err);
  var text = JSON.stringify(wd,null,2);
  fs.writeFileSync("wd_data.json",text,"UTF8",function(err,result){
    console.log("File Written");
  });
})