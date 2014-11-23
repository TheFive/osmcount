fs=require("fs");


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


function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}


exports.readCSV = function (db,defJson,filename,encoding) {
	fs.readFile(filename, 'UTF8', function (err,data) {
		if (typeof(db)=='undefined') {
			console.log("Fehlerhafte Datenbank im ImportCSV");
			return;
		}
		else {
			console.log("Type Of Data "+typeof(data));
		}
		if (err) {
			console.log(err);
			return;
		}
		array = parseCSV(data);
		if(array.length<2) {
			console.log("Emty File "+filename);
			//console.log(array[0]);
			return;
		}
		else {
		
			console.log("Number of Lines in CSV "+array.length);
			console.log("Number of Columns in CSV"+array[0].length)
		}
		for (i=1;i<array.length;i++) {
			if (array[0].length!=array[i].length) {
				console.log("Invalid CSV File, Number of Columns differs "+filename +" Zeile "+i);
				return
			}
		}
		var collection = db.collection('DataCollection');
		newData = [];
		for (i=1;i<array.length;i++) {
			newData[i-1] = clone(defJson);
			for (z=0;z<array[0].length;z++) {
				key = array[0][z];
				value = array[i][z];
				//console.log("---- "+i+" "+z);
				//console.log(key +":"+value);
				newData[i-1][key]=value;
			}
		}
		collection.insert(newData,{w:1},function (){});
		
   	});
}