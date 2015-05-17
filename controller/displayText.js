var path     = require('path');
var fs       = require("fs");

var wochenaufgabe = require('../wochenaufgabe.js');
var htmlPage       = require('../htmlPage.js');


var waStruct = [
{name: "Apotheke",measure:"Apotheke"},
{name: "Adressen ohne Strasse",measure:"AddrWOStreet"}];
	


function generateNavbar() {
	var waSelector = '<li class="dropdown"><a  class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Aufgaben<span class="caret"></span></a>\n'
  waSelector += '<ul class="dropdown-menu" role="menu">\n' 
           
 


	for (var i = 0;i< waStruct.length;i++ ){
		waSelector += '<li><a href="/wa/'+waStruct[i].measure+'.html">'+waStruct[i].name+'</a></li>\n';
	}
	waSelector += '</ul>\n';
	waSelector += '</li>\n';
	return waSelector;

}

exports.main = function(req,res){
	var page = htmlPage.create();
	page.content = fs.readFileSync(path.resolve(__dirname,"..", "html","index.html"));
	//page.menu = fs.readFileSync(path.resolve(__dirname,"..", "html","menu2.html"));
  
  

	page.addNavbarItem(generateNavbar());


	page.footer = "OSM Count...";
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}


exports.wochenaufgabe = function(req,res) {
	var aufgabe = req.param("aufgabe");
	var page = htmlPage.create();


	page.title = wochenaufgabe.map[aufgabe].title;

	page.content = fs.readFileSync(path.resolve(__dirname, "..","html",aufgabe+".html"));
	page.addNavbarItem(generateNavbar());
 	res.set('Content-Type', 'text/html');
	res.end(page.generatePage());
}
