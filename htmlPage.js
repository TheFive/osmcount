var fs = require('fs');
var path = require('path');


var osmCountBrand = '<div class="navbar-header"><a class="navbar-brand" style="font-family:comic sans ms;font-size: 21px; color: #555" href="#">OSM Count</a></div>';

function HtmlPage(type) {
	this.type = type;
	
	this.title = "OSM Count";
	this.footer = "OSM Count ...";
	this.menu = "";
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
    for (var i = 1;i<this.navbar.length;i++) {
      navbar += this.navbar[i];
    }
    navbar += '</ul></div>';

    var content = this.content;
    if (typeof (this.menu) != 'undefined' && this.menu != "") {
      content = this.menu +"<br>"+this.content;
    }

  	
  	var pageFile = path.resolve(__dirname, 'html',this.design);
  	var page = "";
  	page += fs.readFileSync(pageFile);
  	page = page.replace('###NAVBAR###',navbar);
    page = page.replace('###CONTENT###',content);
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


