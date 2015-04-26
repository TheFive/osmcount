var debug = require('debug')('listObject');
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
		for (k in object) {
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

			page =new htmlPage.create("table");
			page.title = "Data Inspector";
			page.menu ="";
			page.content = '<h1>'+collectionName+'</h1><p><table>'+text+'</table></p>';
			res.set('Content-Type', 'text/html');

			if (collectionName == "WorkerQueue") {
				should(obj.length).equal(1);
				var query = {schluessel:obj[0].schluessel,
					                 timestamp:obj[0].timestamp,
					                 measure:obj[0].measure};
				DataCollection.find(query,function handleFindOneObject(err, obj2) {
					debug("handleFindOneObject2");
					if (err) {
						console.log(JSON.stringify(err));
						res.end(page.generatePage());
					} else {

						var text = "<tr><th>Key</th><th>Value</th><tr>";
						text+= listValuesTable("",null,obj2);


						page.content += '<h1>Zugehörige Daten</h1><p><table>'+text+'</table></p>';
						res.end(page.generatePage());
					}



			})} else res.end(page.generatePage());
		}
	})}

