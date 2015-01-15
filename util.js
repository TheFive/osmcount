var configuration = require('./configuration.js');
var num  = require('numeral');
var de       = require('numeral/languages/de');
var debug       = require('debug')('util');
  debug.entry       = require('debug')('util:entry');
  debug.data       = require('debug')('util:data');


// internal Function to duplicate an object
exports.clone = function (obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}

exports.copyObject = function(dest,source) {
	if (source == null) return;
	if (typeof(source) != 'object') return;
    for (var attr in source) {
        if (source.hasOwnProperty(attr)) dest[attr] = source[attr];
    }
}

exports.createWaiter = function(seconds) {
	ms = seconds*1000;
	return function (callback) {
		debug.data("Wait for "+seconds+" seconds");
		setTimeout(callback,ms);
	}
}

exports.waitOneMin = exports.createWaiter(configuration.getValue("waitTime",120));

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