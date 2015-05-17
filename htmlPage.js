var fs = require('fs');
var path = require('path');


var osmCountBrand = '<div class="navbar-header"><a class="navbar-brand" style="font-family:comic sans ms;font-size: 21px; color: #555" href="/index.html">OSM Count</a></div>';


var waStruct = [
{name: "Apotheke",measure:"Apotheke"},
{name: "Adressen ohne Strasse",measure:"AddrWOStreet"}];

var tablesStruct = [
{name:"Apotheke",list:[{name:"Deutschland",measure:"Apotheke"},
                       {name:"Österreich",measure:"Apotheke_AT"},
                       {name:"Deutsche PLZ",measure:"ApothekePLZ_DE"}]},
{name:"Adressen Ohne Straße",measure:"AddrWOStreet"}
];
  


function generateNavbarAufgabe() {
  var waSelector = '<li class="dropdown"><a  class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Aufgaben<span class="caret"></span></a>\n'
  waSelector += '<ul class="dropdown-menu" role="menu">\n' 
           
 


  for (var i = 0;i< waStruct.length;i++ ){
    waSelector += '<li><a href="/wa/'+waStruct[i].measure+'.html">'+waStruct[i].name+'</a></li>\n';
  }
  waSelector += '</ul>\n';
  waSelector += '</li>\n';
  return waSelector;

}
function generateNavbarTabelle() {
  var waSelector = '<li class="dropdown"><a  class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Tabelle<span class="caret"></span></a>\n'
  waSelector += '<ul class="dropdown-menu" role="menu">\n' 
           
 


  for (var i = 0;i< tablesStruct.length;i++ ){
    var entry = tablesStruct[i];
    if (typeof entry.measure != 'undefined') {
      waSelector += '<li><a href="/table/'+entry.measure+'.html">'+entry.name+'</a></li>\n';
    } else {
      if (i>0) waSelector += '<li class="divider">';
      waSelector += '</li><li class="dropdown-header">'+entry.name+'</li>\n';
      for (var z=0;z<entry.list.length;z++) {
        waSelector += '<li><a href="/table/'+entry.list[z].measure+'.html">'+entry.list[z].name+'</a></li>\n';
      }
      if (i<tablesStruct.length-1) waSelector += '<li class="divider">';
    }


  }
  waSelector += '</ul>\n';
  waSelector += '</li>\n';
  return waSelector;

}

function HtmlPage(type) {
	this.type = type;
	
	this.title = "";
	this.footer = "OSM Count ...";
	this.menu = "";
  this.modal = "";
	this.content = "";
	if (type == "table") {
		this.design = "pageTableTemplate.html";
	} else {
		this.design = "pageTableTemplate.html";
	}
  this.navbar = [osmCountBrand];
}



exports.create = function(type) {
	return new HtmlPage(type);
}

HtmlPage.prototype = {
  generatePage: function() {
    var navbar = this.navbar[0];
    navbar += '<div id="navbar" class="navbar-collapse collapse"><ul class="nav navbar-nav">'
    navbar += generateNavbarAufgabe();
    navbar += generateNavbarTabelle();

    navbar += "</ul>";
    navbar += '<ul class="nav navbar-nav navbar-right">';

    for (var i=1;i<this.navbar.length;i++) {
      navbar += this.navbar[i];
    }


    var modalWindow = this.modal;    
    if (this.modal != "") {
      var modalWindowFile = path.resolve(__dirname,"html","modalWindow.html");
      modalWindow = "";
      modalWindow += fs.readFileSync(modalWindowFile);
      modalWindow = modalWindow.replace('###MODALMENU###',this.modal);

      


      navbar += '<li><a href="#" data-toggle="modal" data-target="#myModal"><span class="glyphicon glyphicon-cog" aria-hidden="true"></span></a></li>'
    }

    navbar += '</ul></div>';

    var content = '<h1>'+this.title+'</h1>';
     content += this.content;
  /*  if (typeof (this.menu) != 'undefined' && this.menu != "") {
      content = this.menu +"<br>"+this.content;
    }*/

  	
  	var pageFile = path.resolve(__dirname, 'html',this.design);
  	var page = "";
  	page += fs.readFileSync(pageFile);
  	page = page.replace('###NAVBAR###',navbar);
    page = page.replace('###CONTENT###',content);
    page = page.replace('###MODALWINDOW###',modalWindow);
    page = page.replace('###FOOTER###',this.footer);
    var odblLicense = 'Daten von <a href="http://www.openstreetmap.org/">OpenStreetMap</a> - Veröffentlicht unter <a href="http://opendatacommons.org/licenses/odbl/">ODbL</a>';
    page = page.replace('###ODBLLICENSE###',odblLicense);
  
    // head unused yet

	  return page;
  },
  addNavbarItem : function addNavbarItem(item) {
    this.navbar.push(item);
  }
  
  
}


