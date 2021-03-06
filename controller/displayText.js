var path     = require('path');
var fs       = require("fs");

var wochenaufgabe = require('../wochenaufgabe.js');
var htmlPage       = require('../htmlPage.js');




exports.main = function(req,res){
	var page = htmlPage.create();

	var a = fs.readFileSync(path.resolve(__dirname,"..", "html","osmbcindex.html"),'utf8');
	//page.menu = fs.readFileSync(path.resolve(__dirname,"..", "html","menu2.html"));
	page.content = a.replace('<a href=/index.html>HOME</a>','');
  
  



	page.footer = "OSM Count...";
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}
exports.manual = function(req,res){
	var page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname,"..", "html","manual.html"),'utf8');
  	page.footer = "OSM Count...";
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	var page = htmlPage.create();


	//page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "..","html",aufgabe+".html"));
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}

exports.pharmaciesNotImplemented = function(req,res) {
	var page = htmlPage.create();


	//page.title = wochenaufgabe.map[aufgabe].title;

	page.content = '<h1>Apotheken Liste aktuell nicht implementiert</h1> \
                    Aufgrund von Importproblemen können die Apotheken POIs zur Zeit \
                    nicht angezeigt werden. <br> Ich arbeite dran. Christoph';
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}