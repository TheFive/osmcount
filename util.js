

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
		console.log("Wait for "+seconds+" seconds");
		setTimeout(callback,ms);
	}
}

exports.waitOneMin = exports.createWaiter(120);

