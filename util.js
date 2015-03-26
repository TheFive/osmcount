var configuration = require('./configuration.js');
var num  = require('numeral');
var de       = require('numeral/languages/de');
var debug       = require('debug')('util');
  debug.entry       = require('debug')('util:entry');
  debug.data       = require('debug')('util:data');


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
		debug.data("Wait for "+seconds+" seconds");
		setTimeout(callback,ms);
	}
}
var waitOneMin = exports.createWaiter(configuration.getValue("waitTime",120));
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

exports.toHStore = function(object) {
  var result="";
  for (var k in object) {
    if (result != "" ) result += ",";
    result += '"' + k + '"=>"' +object[k] + '"';
  }
  return result;
}