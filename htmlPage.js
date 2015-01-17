var fs = require('fs');
var path = require('path');

function HtmlPage(type) {
	this.type = type;
	
	this.title = "OSM Count";
	this.footer = "OSM Count ...";
	this.menu = "";
	this.content = "";
	if (type == "table") {
		this.design = "design2.css";
	} else {
		this.design = "design.css";
	}
}
function  getCssStyle(style) {
  	return '<link rel="stylesheet" type="text/css" href="/'+style+'" />'
  }


exports.create = function(type) {
	return new HtmlPage(type);
}

HtmlPage.prototype = {
  generatePage: function() {
  	
  	cssStyle = getCssStyle('table.css') + getCssStyle(this.design);
  	titlePageFile = path.resolve(__dirname, 'html','PageTitle.html');
  	var titlePage = "";
  	titlePage += fs.readFileSync(titlePageFile);
  	titlePage = titlePage.replace('##########',this.title);
  	head = '<head>'+cssStyle+'</head>';
  	pageTitle = '<div id="kopfbereich">'+titlePage+'</div>';
  	pageMenu = '<div id="steuerung">'+this.menu+'</div>';
	pageContent = '<div id="inhalt">'+this.content+'</div>';
	pageFooter = '<div id="fussbereich">'+this.footer+'</div>';

	page = "<html>"+head+
			"<body>" 	+pageTitle
						+pageMenu
						+pageContent
						+pageFooter+"</body></html>";
	return page;
  } 
  
  
}


