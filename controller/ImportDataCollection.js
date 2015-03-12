var debug = require('debug')('ImportDataCollection');
var fs    = require('fs');
var path  = require('path');
var server = require('../server.js')
var async  = require('async')

var DataCollection = require('../model/DataCollection.js');

exports.showPage = function showPage(req,res) {
  debug("exports.showPage");
  // Fetch the collection test
  var importDir = path.resolve(server.dirname, "import")
  var listOfCSV=fs.readdirSync(importDir);
  var list=[];

  for (var i =0;i<listOfCSV.length;i++){
    var filenameLong=path.resolve(importDir,listOfCSV[i]);
    if (fs.statSync(filenameLong).isDirectory()) {
      continue;
    }
    var item = {filename:listOfCSV[i]};
    list.push(item);
  }
  function importOneCSV (item,callback) {

    var filenameLong=path.resolve(importDir,item.filename);
    var filename = item.filename;
    if (fs.statSync(filenameLong).isDirectory()) {
      callback(null,null);
      return;
    }
  	var year = filename.substring(0,4);
  	var month = filename.substring(5,5+2);
  	var day = filename.substring(8,8+2);
  	var date = new Date(year,month-1,day);
  	debug("Datum"+date+"("+year+")("+month+")("+day+")");
  	date.setTime( date.getTime() - date.getTimezoneOffset()*60*1000 );
  	var defJSON = { measure: "AddrWOStreet",
  					count: 0,
  					timestamp:date,
  					execdate:date,
  					source:filename};
  	DataCollection.importCSV(filenameLong, defJSON, function(err,result) {
      if (err) {
        item.err = err;
        callback(null);
        return;
      };
      if (result) {
        item.result = result;
        callback(null);
        return;
      }
      item.result = "UNKLAR !!";
      callback(null);
    });
  }
  async.each(list, importOneCSV, function (err) {
    if (err) {

      text += "Error: "+JSON.stringify(err);
    } else {
      text = "ImportDir: "+importDir+"\n";
      text += "Files: "+JSON.stringify(list)+"\n";

      text = "Files imported"+JSON.stringify(list);
    }
    res.set('Content-Type', 'text/html');
    res.end(text);
  })
}
