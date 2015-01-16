var fs = require('fs');

function HtmlPage(type) {
	this.type = type;
	var titlePage = path.resolve(__dirname, 'html','title.html');
	this.title = fs.readFileSync(titlePage);
	this.footer = "OSM Count ...";
	this.menu = "";
	this.content = "";
	if (type == "table") {
		this.design = "design2.css";
	} else {
		this.design = "design.css";
	}
}

HtmlPage.prototype = {
  function generatePage () {
  	
  	cssStyle = this.getCssStyle('table.css') +this.getCssStyle(this.design);
  	head = '<head>'+cssStyle+'</head>';
  	pageTitle = '<div id="kopfbereich">'+this.title+'</div>';
  	pageMenu = '<div id="steuerung">'+this.menu+'</div>';
	pageContent = '<div id="fussbereich">'+this.content+'</div>';
	pageFooter = '<div id="fussbereich">'+this.footer+'</div>';

	page = "<html>"+head+
			"<body>" 	+pageTitle
						+pageMenu
						+pageContent
						+pageFooter+"</body></html>";
	return page;
  } 
  function getCssStyle(style) {
  	return '<link rel="stylesheet" type="text/css" href="/'+style+'" />'
  }
  
  
}


