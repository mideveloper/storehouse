/**
 * Created by faiz on 2/11/16.
 */

var _ = require("lodash");

function deleteAllPropertiesWhichAreUndefined(object) {
	if(_.isObject(object)) {
		var keys = Object.keys(object);
		var i = 0, length = keys.length;
		for (;i<length;i++) {
			if (object[keys[i]] === undefined) {
				delete object[keys[i]];
			}
		}
	}
	return object;
}

module.exports.deleteAllPropertiesWhichAreUndefined = deleteAllPropertiesWhichAreUndefined;
