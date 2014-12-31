fs=require("fs");
var util    = require('./util.js');
var path    = require('path');
var debug   = require('debug')('importCSV');
 debug.entry = require('debug')('importCSV:entry');
 debug.data = require('debug')('importCSV:data');



// Code for parsing simple CSV Files including ""
// Code taken from: http://stackoverflow.com/questions/1293147/javascript-code-to-parse-csv-data
// and partially modified

// Input: string containing CSV Data
// Output: Array of JSON Objects

function parseCSV(str,delimiter) {
	debug.entry("parseCSV(...,"+delimiter+")");
	
    var arr = [];
    var quote = false;  // true means we're inside a quoted field

    // iterate over each character, keep track of current row and column (of the returned array)
    for (var row = col = c = 0; c < str.length; c++) {
        var cc = str[c], nc = str[c+1];        // current character, next character
        arr[row] = arr[row] || [];             // create a new row if necessary
        arr[row][col] = arr[row][col] || '';   // create a new column (start with empty string) if necessary

        // If the current character is a quotation mark, and we're inside a
        // quoted field, and the next character is also a quotation mark,
        // add a quotation mark to the current column and skip the next character
        if (cc == '"' && quote && nc == '"') { arr[row][col] += cc; ++c; continue; }  

        // If it's just one quotation mark, begin/end quoted field
        if (cc == '"') { quote = !quote; continue; }

        // If it's a comma and we're not in a quoted field, move on to the next column
        if (cc == delimiter && !quote) { col += 1; continue; }

        // If it's a newline and we're not in a quoted field, move on to the next
        // row and move to column 0 of that new row
        if (cc == '\n' && !quote) { row +=1; col = 0; continue; }
        if (cc == '\r' && !quote) continue; //just ignore this character

        // Otherwise, append the current character to the current column
        arr[row][col] += cc;
    }
    debug.entry("leave: parseCSV(...,"+delimiter+")"+"read "+arr.length+" Elements");
    return arr;
}


exports.parseCSV = parseCSV;

exports.convertArrToJSON = function(array,defJson) {
	debug.entry("convertArrToJSON");
	numeral = util.numeral;
	var newData = [];
	debug.data("Structure "+JSON.stringify(array[0]));
	for (i=1;i<array.length;i++) {
		debug.data("Convert Line "+i);
		newData[i-1] = util.clone(defJson);
		
		for (z=0;z<array[0].length;z++) {
			debug.data("Convert Column "+z);
			key = array[0][z];
			value = array[i][z];
			debug.data(key+typeof(defJson[key]));
			switch(typeof(defJson[key])) {
				case 'number':
					newData [i-1][key]=numeral().unformat(value);
					break;
				case 'date':
					y=value.slice(0,4);
					m=value.slice(6,6+2);
					d=value.slice(9,9+2);
					newData[i-1][key]=new date(y,m-1,d);
					break;
				default:
					newData[i-1][key]=value;
			}		
		}
	}
	return newData;
}

function importCSVToCollection(filename,collection,defJson,cb) {
	debug.entry("importCSVToCollection("+filename+"..)");

	
	// Open the file and copy it to a string
	// encoding is ignored yet
	fs.readFile(filename, 'UTF8', function (err,data) {
		debug.entry("importCSVToCollection readFileCB" + filename);

		
		if (err) {
			console.log(err);
			if (cb) cb(err,null);
			return;
		}
		
		// convert the content of the file to an JSON array
		array = parseCSV(data,";");
		
		if(array.length<2) {
			// CSV File is empty, log an error
			console.log("Empty File %s",filename);
			if (cb) cb("empty file",null);
			return;
		}
		else {
		    // just log the column and rows for debugging reasons
			debug("Number of Lines in CSV "+array.length);
			debug("Number of Columns in CSV"+array[0].length)
		}
		
		// Quality Check, does all rows has the same count of columns ?
		for (i=1;i<array.length;i++) {
			if (array[0].length!=array[i].length) {
				debug("Invalid CSV File, Number of Columns differs "+filename +" Zeile "+i);
				if (cb) cb("Invalid CSV File",null);
				return
			}
		}
		
		// Copy measures into the DataCollection MongoDB
		var newData = exports.convertArrToJSON(array,defJson);

		collection.insert(newData,{w:1},function (){if (cb) cb(null,null);});
   	});}


// readCSV Function to import CSV to the Measure Database
// Input: a MongoDB, a template JSON, CSV Filename, encoding 
exports.readCSV = function(filename,db,defJson,cb) {
	debug.entry("readCSV("+filename+"..)");


		var collection = db.collection('DataCollection');
		importCSVToCollection(filename,collection,defJson,cb);
		var newData = this.convertArrToJSON(arr,defJson);

		
}

exports.importApothekenVorgabe = function(db,cb) {
	debug.entry("importApothekenVorgabe("+"..)");
	var  defJson = {
		source: "ABDA2011+Schätzung",
		measure: "Apotheke",
		apothekenVorgabe: 0.0
	}; 
	var filename = "Anzahl Apotheken.csv";
	filename = path.resolve(__dirname, filename);
	var collection = db.collection('DataTarget');
	importCSVToCollection(filename,collection,defJson,cb);
		
}