var config = require('./configuration.js');
var num    = require('numeral');
var de     = require('numeral/languages/de');
var debug  = require('debug')('util');


exports.copyObject = function(dest,source) {
	if (source == null) return;
	if (typeof(source) != 'object') return;
    for (var attr in source) {
        if (source.hasOwnProperty(attr)) dest[attr] = source[attr];
    }
}

// internal Function to duplicate an object
exports.clone = function (obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    exports.copyObject(copy,obj);
    return copy;
}


exports.createWaiter = function(seconds) {
	var ms = seconds*1000;
	return function (callback) {
		debug("Wait for "+seconds+" seconds");
		setTimeout(callback,ms);
	}
}
var waitOneMin = exports.createWaiter(config.getValue("waitTime",120));
exports.waitOneMin = waitOneMin;

exports.numeral = num;

exports.initialise = function() {
    num.language('de', {
        delimiters: {
            thousands: '.',
            decimal: ','
        },
        abbreviations: {
            thousand: 'k',
            million: 'm',
            billion: 'b',
            trillion: 't'
        },
        ordinal: function (number) {
            return '.';
        },
        currency: {
            symbol: '€'
        }
    });
	num.language('de');
}

function escapeString(string) {
	var result='';
	for (var i=0;i<string.length;i++) {
		if (string[i]=='"') {
			result += '\\\"';
			continue;
		}
		if (string[i]=='\\') {
			result += '\\\\';
			continue;
		}
		result += string[i];
  }
  return result;
}
exports.toHStore = function(object) {
  var result='';
  for (var k in object) {
    if (result != "" ) result += ",";
    var value = "" + object[k];
    result += '"' + k + '"=>"' +escapeString(value) + '"';
  }
  return result;
}