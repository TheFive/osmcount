fs=require("fs");


// Code for parsing simple CSV Files including ""
// Code taken from: http://stackoverflow.com/questions/1293147/javascript-code-to-parse-csv-data
// and partially modified

// Input: string containing CSV Data
// Output: Array of JSON Objects

function parseCSV(str) {
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
        if (cc == ',' && !quote) { col += 1; continue; }

        // If it's a newline and we're not in a quoted field, move on to the next
        // row and move to column 0 of that new row
        if (cc == '\n' && !quote) { row +=1; col = 0; continue; }

        // Otherwise, append the current character to the current column
        arr[row][col] += cc;
    }
    return arr;
}

// internal Function to duplicate an object
function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}


// readCSV Function to import CSV to the Measure Database
// Input: a MongoDB, a template JSON, CSV Filename, encoding 
exports.readCSV = function (db,defJson,filename,encoding) {
	
	// Open the file and copy it to a string
	// encoding is ignored yet
	fs.readFile(filename, 'UTF8', function (err,data) {
		if (typeof(db)=='undefined') {
			console.log("Internal Error db undefined");
			return;
		}
		
		if (err) {
			console.log(err);
			return;
		}
		
		// convert the content of the file to an JSON array
		array = parseCSV(data);
		
		if(array.length<2) {
			// CSV File is empty, log an error
			console.log("Empty File "+filename);
			return;
		}
		else {
		    // just log the column and rows for debugging reasons
			console.log("Number of Lines in CSV "+array.length);
			console.log("Number of Columns in CSV"+array[0].length)
		}
		
		// Quality Check, does all rows has the same count of columns ?
		for (i=1;i<array.length;i++) {
			if (array[0].length!=array[i].length) {
				console.log("Invalid CSV File, Number of Columns differs "+filename +" Zeile "+i);
				return
			}
		}
		
		// Copy measures into the DataCollection MongoDB
		var collection = db.collection('DataCollection');
		newData = [];
		for (i=1;i<array.length;i++) {
			newData[i-1] = clone(defJson);
			for (z=0;z<array[0].length;z++) {
				key = array[0][z];
				value = array[i][z];
				
				//console.log("---- "+i+" "+z);
				//console.log(key +":"+value);
				if (typeof(defJSON)!='integer') {
					//console.log(key + typeof(newData[value]));
					newData[i-1][key]=value;
				} else {
					newData [i-1]=Integer.parseInt(value);
				}
			}
		}
		collection.insert(newData,{w:1},function (){});
   	});
}