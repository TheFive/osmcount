var debug   = require('debug')('importCSV');
var fs      = require("fs");
var path    = require('path');
var pg      = require('pg');
var async   = require('async');
var debug   = require('debug')('ImportCSV');
var config  = require('./configuration.js');
var util    = require('./util.js');


// Code for parsing simple CSV Files including ""
// Code taken from: http://stackoverflow.com/questions/1293147/javascript-code-to-parse-csv-data
// and partially modified

// Input: string containing CSV Data
// Output: Array of JSON Objects

// Error Handling: none

function parseCSV(str,delimiter) {
	debug("parseCSV(...,"+delimiter+")");

    var arr = [];
    var quote = false;  // true means we're inside a quoted field
    var row,col,c;
    // iterate over each character, keep track of current row and column (of the returned array)
    for (row = col = c = 0; c < str.length; c++) {
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
        if (cc == '\r' && nc == '\n' && !quote) { row +=1; col = 0; ++c; continue; }
        if ( ((cc == '\r')||(cc == '\n')) && !quote) { row +=1; col = 0; continue; }

        // Otherwise, append the current character to the current column
        arr[row][col] += cc;
    }
    debug("leave: parseCSV(...,"+delimiter+")"+"read "+arr.length+" Elements");
    return arr;
}


exports.parseCSV = parseCSV;

// Code for converting an array (created by parseCSV) to a JSON Array.
// Input: an array generated by parseCSV
// Input: a "Default" JSON Object, that is used to create each JSON Object
// Output: Function returns an array of JSON Objects
// Error Handling: None

exports.convertArrToJSON = function(array,defJson) {
	debug("convertArrToJSON");
	var numeral = util.numeral;
	var newData = [];
	debug("Structure "+JSON.stringify(array[0]));
	for (var i=1;i<array.length;i++) {
		debug("Convert Line "+i);
		newData[i-1] = util.clone(defJson);

		for (var z=0;z<array[0].length;z++) {
			debug("Convert Column "+z);
			var key = array[0][z];
			var value = array[i][z];
			debug(key+typeof(defJson[key]));
			if (defJson[key] instanceof Date) {
				var y=parseInt(value.slice(0,4));
				var m=parseInt(value.slice(5,6+2));
				var d=parseInt(value.slice(8,9+2));
				newData[i-1][key]=new Date(y,m-1,d,0,0,0);
				continue;
			}
			switch(typeof(defJson[key])) {
				case 'number':
					newData [i-1][key]=numeral().unformat(value);
					break;
				case 'date':

					break;
				default:
					newData[i-1][key]=value;
			}
		}
	}
	return newData;
}

// Import a CSV File, check it, and convert it to JSON
// Input: filename, default JSON, and a callback
// Output: callback is called with error status and result.

function importCSVFileToJSON(filename,defJson,cb) {
	debug("importCSVFileToJSON("+filename+"..)");

	// Open the file and copy it to a string
	// encoding is ignored yet
	fs.readFile(filename, 'UTF8', function importCSVFileToJSONReadFileCB(err,data) {
		debug("importCSVFileToJSONReadFileCB" + filename);
		if (err) {
			if (cb) cb(err,null);
			return;
		}

		// convert the content of the file to an JSON array
		var array = parseCSV(data,";");

		if(array.length<2) {
			// CSV File is empty, log an error
			if (cb) cb("empty file",null);
			return;
		}
		else {
		    // just log the column and rows for debugging reasons
			debug("Number of Lines in CSV "+array.length);
			debug("Number of Columns in CSV"+array[0].length)
		}

		// Quality Check, does all rows has the same count of columns ?
		for (var i=1;i<array.length;i++) {
			if (array[0].length!=array[i].length) {
				var error = "Invalid CSV File, Number of Columns differs "+filename +" Zeile "+i
				if (cb) cb(error,null);
				return
			}
		}

    // after the check, convert the array to JSON
		var newData = exports.convertArrToJSON(array,defJson);
    cb(null,newData);
  });
}

exports.importCSVFileToJSON = importCSVFileToJSON;

/*

Source skipped, has to be moved to DataTarget.

exports.importApothekenVorgabe = function(measure,db,cb) {
	debug("importApothekenVorgabe("+measure+"..)");
	var defJson = {measure:measure,apothekenVorgabe :0.0};
	var filename = "Anzahl "+measure+".csv";
	filename = path.resolve(__dirname, filename);
	var collection = db.collection('DataTarget');
	importCSVToMongoCollection(filename,collection,defJson,cb);

}*/
