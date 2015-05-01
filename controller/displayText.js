var path     = require('path');
var fs       = require("fs");

var wochenaufgabe = require('../wochenaufgabe.js');
var htmlPage       = require('../htmlPage.js');


exports.main = function(req,res){
	var page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname,"..", "html","index.html"));
	page.menu = fs.readFileSync(path.resolve(__dirname,"..", "html","menu.html"));
	page.footer = "OSM Count...";
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	var page = htmlPage.create();

	page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "..","html",aufgabe+".html"));
	page.menu = fs.readFileSync(path.resolve(__dirname, "html","menu.html"));
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}
