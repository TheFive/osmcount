var fs = require('fs');
var path = require('path');


var htmlStart = ' \
<!DOCTYPE html> \n \
<html lang="en"> \n \
  <head> \n \
    <meta charset="utf-8"> \n \
    <meta http-equiv="X-UA-Compatible" content="IE=edge"> \n \
    <meta name="viewport" content="width=device-width, initial-scale=1"> \n \
    <!-- The above 3 meta tags *must* come first in the head; any other head content must come *after* these tags -->\n \
    <title>OSMCOUNT</title> \n \
\n \
    <!-- Bootstrap --> \n \
    <link href="css/bootstrap.min.css" rel="stylesheet"> \n \
 \n \
    <!-- Custom CSS -->  \n \
    <link href="css/simple-sidebar.css" rel="stylesheet"> \n \
\n \
\n \
    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries --> \n \
    <!-- WARNING: Respond.js doesnt work if you view the page via file:// -->  \n \
    <!--[if lt IE 9]> \n \
      <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script> \n \
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script> \n \
    <![endif]--> \n \
  </head> \n \
  <body>  \n \
  <div id="wrapper">'

var htmlEnd = '\
 </div> \
        <!-- /#page-content-wrapper --> \
    <!-- jQuery (necessary for Bootstraps JavaScript plugins) -->\
   <script src="js/jquery.js"></script>\
    <!-- Include all compiled plugins (below), or include individual files as needed -->\
    <script src="js/bootstrap.min.js"></script>\
  </body> \
</html>'


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
  	
  	cssStyle =   getCssStyle(this.design);
  	titlePageFile = path.resolve(__dirname, 'html','PageTitle.html');
  	var titlePage = "";
  	titlePage += fs.readFileSync(titlePageFile);
  	titlePage = titlePage.replace('##########',this.title);
  	head = '<head>'+cssStyle+'</head>';
  	pageTitle = '<div id="kopfbereich">'+titlePage+'</div>';
    pageTitle = "";
  	pageMenu = '<div id="sidebar-wrapper">'+this.menu+'</div>';
	pageContent = '<div id="page-content-wrapper"><div class="container-fluid"><div class="row"><div class="col-lg-12">'+this.content+'</div></div></div></div>';
	pageFooter = '<div id="fussbereich">'+this.footer+'</div>';
	odblLicense = 'Daten von <a href="http://www.openstreetmap.org/">OpenStreetMap</a> - Veröffentlicht unter <a href="http://opendatacommons.org/licenses/odbl/">ODbL</a>';

    // head unused yet


	page = htmlStart	+pageTitle
						+pageMenu
						+pageContent+
					//	pageFooter+
					//	odblLicense+
					htmlEnd;
	return page;
  } 
  
  
}


