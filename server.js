var express = require('express');
var app = express();

//require node modules (see package.json)
var MongoClient = require('mongodb').MongoClient;
var format = require('util').format;






app.use(function(req, res, next){
    console.log(req.url);
    next();
});
app.use('/count.html', function(req,res){


MongoClient.connect('mongodb://127.0.0.1:27017/mosmcount', function(err, db) {
  if (err) throw err;
  console.log("Connected to Database mosmcount");
  
    // Fetch the collection test
    var collection = db.collection('DataCollection');
    collection.count(function(err, count) {
          res.end("There are " + count + " records.");
        });
        
       
        
        
        

	})
	})
 
app.use('/', express.static(__dirname));
app.get('/*', function(req, res) {
    res.status(404).sendFile(__dirname + '/error.html');
});
app.listen(64163);

console.log("server has started and is listening to http://127.0.0.1:64163");
	