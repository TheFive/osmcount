var debug = require('debug')('displayObject');
var should = require('should');

var htmlPage     = require('../htmlPage.js');


var DataCollection = require('../model/DataCollection.js');
var DataTarget     = require('../model/DataTarget.js');
var WorkerQueue    = require('../model/WorkerQueue.js');




function listValuesTable(keyname,key,object) {
    if (key == '_id') return "";
    if (key == 'id') return "";
	if (object instanceof Date) {
		return "<tr><td>"+keyname+"</td><td>"+object.toString()+"</td><td>date</td></tr>";
	}
/*	if (object instanceof ObjectID) {
		return "<tr><td>"+keyname+"</td><td>"+object+"</td></tr>";
	}*/
	if (typeof(object) == 'object') {
		var result = "";
		for (var k in object) {
			if (key) {
			  result += listValuesTable(key+"."+k,k,object[k]);
			} else {
			  result += listValuesTable(k,k,object[k]);
			}

		}
		return result;
    }
    return "<tr><td>"+keyname+"</td><td>"+object+"</td><td>"+typeof(object)+"</td></tr>";

}

exports.object = function(req,res) {
	debug("exports.object");
	var db = res.db;

	var collectionName = req.params["collection"];
	var objid = req.params["id"];
	//console.log(objid);
	var collection;
    switch(collectionName) {
        case "DataTarget": collection=DataTarget;break;
        case "DataCollection": collection = DataCollection;break;
        case "WorkerQueue": collection = WorkerQueue;break;
    }

	collection.find ({id:objid},{},function handleFindOneObject(err, obj) {
		debug("handleFindOneObject");
		if (err) {
			var text = "Display Object "+ objid + " in Collection "+collectionName;
			text += "Error: "+JSON.stringify(err);
			res.set('Content-Type', 'text/html');
			res.end(text);
		} else {
			text = "<tr><th>Key</th><th>Value</th><tr>";
			text+= listValuesTable("",null,obj);

			var page =new htmlPage.create("table");
			page.title = "Data Inspector";
			page.menu ="";
			page.content = '<h1>'+collectionName+'</h1><p><table class="table-condensed table-bordered table-hover">'+text+'</table></p>';
			res.set('Content-Type', 'text/html');

			if (collectionName == "WorkerQueue") {
				should(obj.length).equal(1);
				var query = {schluessel:obj[0].schluessel,
					                 timestamp:obj[0].exectime,
					                 measure:obj[0].measure};
				DataCollection.find(query,function handleFindOneObject(err, obj2) {
					debug("handleFindOneObject2");
					if (err) {
						console.log(JSON.stringify(err));
						res.end(page.generatePage());
					} else {

						var text = "<tr><th>Key</th><th>Value</th><tr>";
						text+= listValuesTable("",null,obj2);

						var menuString = "";
						if (obj2.length ==0 && obj[0].status == 'error') {
							menuString = '<form action ="IgnoreError" > \
							Fehler Ignorieren, Grund: \
							<select name="reason"> \
							<option value="NotExcecuted" selected>Nicht Ausgeführt</option> \
								</select> \
							<select name="object"> \
							<option value="'+obj[0].id+'" selected>Object ID</option> \
								</select> \
  										<input type="submit" value="Ignore Error"> \
										</form> '
						}
						page.menu = menuString;


						page.content += '<h1>Zugehörige Daten</h1><p><table class="table-condensed table-bordered table-hover">'+text+'</table></p>';
						res.end(page.generatePage());
					}



			})} else res.end(page.generatePage());
		}
	})}


exports.ignoreError = function(req,res) {
	debug("exports.ignoreError");
	var db = res.db;

	var objid = req.query["object"];
	var reason = req.query["reason"];



	WorkerQueue.find ({id:objid},{},function handleFindOneObject(err, obj) {
		debug("handleFindOneObject");
		if (err) {
			var text = "Display Object "+ objid + " in Collection "+collectionName;
			text += "Error: "+JSON.stringify(err);
			res.set('Content-Type', 'text/html');
			res.end(text);
		} else {
			should(obj.length).equal(1);
			var item = obj[0];
			var query = {schluessel:obj[0].schluessel,
				                 timestamp:obj[0].exectime,
				                 measure:obj[0].measure};
			DataCollection.find(query,function handleFindOneObject(err, obj2) {
				debug("handleFindOneObject2");
				var page =new htmlPage.create("table");
				page.title = "Data Inspector Correct Error Page";
			  page.menu ="";
			  res.set('Content-Type', 'text/html');

	

				if (err) {
					console.log(JSON.stringify(err));
					res.end(page.generatePage());
				} else {
					if (obj2.length ==0 && item.status == 'error') {
						// alles gecheckt, object Ändern
						item.status = 'open';
						var errorMessage;
						if (typeof(item.error.message)!='undefined') {
							errorMessage = item.error.message;
						}
						if (typeof(item.error.code)!='undefined') {
							errorMessage = item.error.code;
						}
						item.error.fix = "Was Error Code "+errorMessage+ " Fixed for reason "+ reason;
						WorkerQueue.saveTask(item,function(err) {
							if (!err) {
								req.params.collection= "WorkerQueue";
								req.params.id=item.id;
								exports.object(req,res);
								return;
							} else {
								page.content = "<h1>Postgres Error</h1><pre>"+JSON.stringify(err)+"</pre>";
							}
							res.end(page.generatePage());
							return;
						})
					} else {
						page.content = "Fehler kann nicht korrigiert werden";
						res.end(page.generatePage());
					}
				}
			})
		}
	})
}
