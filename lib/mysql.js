var Knex = require("knex"),
	_ = require("lodash"),
	CommonHelper = require("./common");

function initialize(params) {

	/*jshint newcap: false  */

	var _knex = Knex({
		client: "mysql",
		connection: {
			host: params.host,
			user: params.uid,
			password: params.pwd,
			database: params.db,
			charset: (params.charset) ? params.charset : "UTF8_GENERAL_CI",
			multipleStatements: _.has(params, "isMultiStatement") ? params.isMultiStatement : false
		}
	});

	function zeroPad(number, length) {
		number = number.toString();
		while (number.length < length) {
			number = "0" + number;
		}

		return number;
	}

	function dateToString(date) {
		var dt = new Date(date);
		dt.setTime(dt.getTime() + (dt.getTimezoneOffset() * 60000));

		var year = dt.getFullYear();
		var month = zeroPad(dt.getMonth() + 1, 2);
		var day = zeroPad(dt.getDate(), 2);
		var hour = zeroPad(dt.getHours(), 2);
		var minute = zeroPad(dt.getMinutes(), 2);
		var second = zeroPad(dt.getSeconds(), 2);
		var millisecond = zeroPad(dt.getMilliseconds(), 3);

		return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second + "." + millisecond;
	}



	function escape(val) {
		if (val === undefined || val === null) {
			return "NULL";
		}

		switch (typeof val) {
			case "boolean":
				return (val) ? "true" : "false";
			case "number":
				return val + "";
		}

		if (val instanceof Date) {
			val = dateToString(val);
		}


		val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
			switch (s) {
				case "\0":
					return "\\0";
				case "\n":
					return "\\n";
				case "\r":
					return "\\r";
				case "\b":
					return "\\b";
				case "\t":
					return "\\t";
				case "\x1a":
					return "\\Z";
				default:
					return "\\" + s;
			}
		});

		return val;
	}

	function escapeKeys(key) {
		//its not all
		var keyList = ["key", "add", "and", "before", "by",
			"call", "case", "condition", "delete","desc",
			"describe", "from", "group", "in", "index",
			"insert", "interval", "in", "key", "like",
			"limit", "long", "match", "not", "option",
			"or", "order", "references", "select", "table", "to", "update", "where"];
		if(keyList.indexOf(key.toLowerCase()) > -1) {
			return "`" + key + "`";
		}
		else {
			return key;
		}
	}

	function getInsertSQL(ctx, object) {


		var isFirst = true;
		var dup = "";
		var sql = "INSERT INTO " + ctx.tableName + " SET ";

		for (var prop in object) {

			if (!_.isUndefined(object[prop]) && !_.isNull(object[prop])) {
				var column = escapeKeys(prop);
				if (isFirst) {
					isFirst = false;
				}
				else {
					sql += ", ";
					dup += ", ";
				}
				sql += column + " = '" + escape(object[prop]) + "'";
				dup += column + " = values(" + column + ")";
			}
		}


		sql += " ON DUPLICATE KEY UPDATE " + dup + ";";
		return sql;
	}

	function extend(attributes) {
		if (!attributes.tableName) {
			throw new Error("tableName must be defined");
		}

		function Model(obj) {

			//if model creation is not with new keyword then return it with new
			if (!(this instanceof Model)) {
				return new Model(obj);
			}

			this.tableName = attributes.tableName;

			this.input = obj;
		}

		Model.prototype.update = function update(where_clause, set_fields) {
			var self = this;

			return _knex(self.tableName).where(where_clause).update(set_fields);

		};

		Model.prototype.save = function save() {
			var self = this;
			return _knex.raw(getInsertSQL(self, CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input))));
		};

		Model.prototype.saveInBatch = function saveInBatch() {
			var self = this;
			var batch_statement = "";

			for (var index = 0; index < self.input.length; index++) {
				batch_statement += getInsertSQL(self, CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input[index])));
			}

			return _knex.raw(batch_statement);

		};



		Model.prototype.fetch = function fetch() {
			var self = this;
			if (arguments.length > 0) {

				return _knex(self.tableName).where(CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input))).select(_.toArray(arguments)).then(function(obj) {
					if (obj.length > 0) {
						return self.getObjectFromDBObject(obj.shift());
					}
					else {
						return null;
					}
				});
			}
			else {
				return _knex(self.tableName).where(CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input))).select().then(function(obj) {
					if (obj.length > 0) {
						return self.getObjectFromDBObject(obj.shift());
					}
					else {
						return null;
					}
				});
			}

		};

		Model.prototype.getInBatch = function getInBatch() {
			var self = this;
			var result = [];
			return _knex(self.tableName).whereIn(((self.idAttribute) ? self.idAttribute : "id"), self.getDBObject(self.input)).then(function(output) {

				while (output.length > 0) {
					result.push(self.getObjectFromDBObject(output.shift()));
				}

				return result;
			});
		};


		Model.prototype.find = function find(criteria, select_clause) {
			var self = this;
			var result = [];
			if (!criteria) {
				return _knex(self.tableName).select(select_clause).then(function(output) {
					while (output.length > 0) {
						result.push(self.getObjectFromDBObject(output.shift()));
					}
					return result;
				});
			}
			else {
				return _knex(self.tableName).where(criteria).select(select_clause).then(function(output) {
					while (output.length > 0) {
						result.push(self.getObjectFromDBObject(output.shift()));
					}
					return result;
				});
			}

		};

		Model.prototype.pagedFind = function pagedFind(criteria, select_clause, skip, limit) {

			var self = this;
			var result = [];
			if (!criteria) {
				return _knex(self.tableName).select(select_clause).limit(limit).offset(skip).then(function(output) {
					while (output.length > 0) {
						result.push(self.getObjectFromDBObject(output.shift()));
					}
					return result;
				});
			}
			else {
				return _knex(self.tableName).where(criteria).select(select_clause).limit(limit).offset(skip).then(function(output) {
					while (output.length > 0) {
						result.push(self.getObjectFromDBObject(output.shift()));
					}
					return result;
				});
			}

		};

		Model.prototype.getDBObject = function getDBObject(obj) {
			return obj;
		};

		Model.prototype.getObjectFromDBObject = function getObjectFromDBObject(obj) {
			return obj;
		};

		Model.prototype.deleteAll = function deleteAll() {
			var self = this;
			return _knex.raw("delete from " + self.tableName).then(function() {
				return true;
			});
		};


		Model.prototype.deleteObject = function deleteObject() {
			var self = this;
			return _knex(self.tableName).where(
				CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input))).del();
		};



		return Model;
	}

	return {
		extend: extend,
		client: _knex

	};


}

module.exports.initialize = initialize;
