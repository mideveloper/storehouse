var ElasticSearch = require("elasticsearch");
var Promise = require("bluebird");

/**
 *
 * Use to initiate the client
 *
 * @author Faiz <faizulhaque@tenpearls.com>
 * @param params
 * @returns {{extend: extend, client: exports.Client}}
 * @version 0.1
 */
function initialize(params){

    //{log: 'trace'}
    //to make consistency with other models
    var connectionString = {host: params.host + ":" + params.port};

    //currently only trace log supported
    if(params.log === "trace") {
        connectionString.log = params.log;
    }
    var _es = new ElasticSearch.Client(connectionString);

    /**
     *
     * to clear undefined value from object
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param object
     * @returns {Object}
     * @version 0.1
     *
     */
    function deleteAllPropertiesWhichAreUndefined(object) {
        for (var k in object) {
            if (object[k] === undefined) {
                delete object[k];
            }
        }
        return object;
    }

    /**
     *
     * Prepare fetch response object
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param self
     * @param error
     * @param response
     * @param resolve
     * @param reject
     * @version 0.1
     */
    function prepareFetchResponseObject(self, error, response, resolve, reject) {
        if(error) {
            if(error.message === "Not Found") {
                return resolve(null);
            }
            return reject(error);
        }
        if(self.arg.length > 0) {
            var dataObject = {};
            for(var k in response.fields) {
                dataObject[k] = response.fields[k][0];
            }
            return resolve(self.getObjectFromDBObject(dataObject));
        }
        else {
            if(response._source) {
                return resolve(self.getObjectFromDBObject(response._source));
            }
            else if(response.hits.hits.length > 0) {
                return resolve(self.getObjectFromDBObject(response.hits.hits.shift()._source));
            }
            else {
                return resolve(null);
            }
        }
    }

    /**
     * Prepare raw raponse format (used for delete)
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param self
     * @param error
     * @param response
     * @param resolve
     * @param reject
     * @returns {*}
     * @version 0.1
     */
    function prepareRawResponse(self, error, response, resolve, reject) {
        if(error) {
            return reject(error);
        }
        return resolve(response);
    }

    /**
     *
     * prepare Query object (for searching)
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param queryObject
     * @param inputObject
     * @version 0.1

     */
    function prepareQueryObject(queryObject, resource){
        var tem = {term : {}};
        queryObject["body"] = { query :{ filtered :{ filter :{bool: {must: []}}}}};
        for (var key in resource) {
            tem.term[key] = resource[key];
            queryObject["body"]["query"]["filtered"]["filter"]["bool"]["must"].push(tem);
        }
    }

    /**
     *
     * prepare update query
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param queryObject
     * @param {Object}
     * @version 0.1
     */
    function prepareUpdateQueryObject(resource) {
        var updateObject = {
            doc: {}
        };
        for (var key in resource) {
            if (resource[key]) {
                updateObject["doc"][key] = resource[key];
            }
        }
        return updateObject;
    }

    /**
     *
     * Prepare fields array (array of string)
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param arg
     * @returns {{}}
     * @version 0.1
     */
    function prepareSelectClause(queryObject, arg) {
        if(arg) {
            queryObject["fields"] = [];
            for (var i = 0; i < arg.length; i++) {
                queryObject["fields"].push(arg[i]);
            }
        }
    }

    /**
     *
     * expose function to outside
     *
     * @author Faiz <faizulhaque@tenpearls.com>
     * @param attributes
     * @returns {Model}
     * @version 0.1
     */
    function extend(attributes) {

        if (!attributes.tableName) {
            throw new Error("tableName must be defined");
        }

        if (!attributes.tableType) {
            throw new Error("tableType must be defined");
        }

        function saveData(context, input) {

            return new Promise(function(resolve, reject) {

                var resource = deleteAllPropertiesWhichAreUndefined(context.getDBObject(input));
                var saveObject = {
                    index: context.tableName,
                    type: context.tableType,
                    body: resource
                };
                if(resource.id) {
                    saveObject.id = resource.id;
                }

                _es.index(saveObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    return resolve(response._id);
                });

            });

        }

        function Model(obj) {

            //if model creation is not with new keyword then return it with new
            if (!(this instanceof Model)) {
                return new Model(obj);
            }

            //tableName will refer to index
            this.tableName = attributes.tableName;

            //tableType will refer to type
            this.tableType = attributes.tableType;

            this.input = obj;
        }

        Model.prototype.fetch = function fetch(){
            var self = this;
            self.arg = arguments;
            return new Promise(function(resolve, reject) {

                var resource = {};
                if(self.input) {
                    resource = deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input));
                }
                var queryObject = {
                    index: self.tableName,
                    type: self.tableType,
                    from : 0,
                    size : 1
                };
                if(self.arg.length > 0) {
                    prepareSelectClause(queryObject, self.arg);
                }
                if (resource.id) {
                    queryObject.id = resource.id;
                    _es.get(queryObject, function(error, response) {
                        prepareFetchResponseObject(self, error, response, resolve, reject);
                    });

                }
                else {
                    prepareQueryObject(queryObject, resource);
                    _es.search(queryObject, function(error, response) {
                        prepareFetchResponseObject(self, error, response, resolve, reject);
                    });
                }
            });
        };

        Model.prototype.find = function find(criteria, selectClause){
            var self = this;
            return new Promise(function(resolve, reject) {

                var resource = {};
                if(criteria) {
                    resource = deleteAllPropertiesWhichAreUndefined(self.getDBObject(criteria));
                }
                var queryObject = {
                    index: self.tableName,
                    type: self.tableType,
                    from: 0,
                    size: 99999
                };
                if(selectClause && selectClause.length > 0) {
                    prepareSelectClause(queryObject, selectClause);
                }
                prepareQueryObject(queryObject, resource);
                _es.search(queryObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    var result = [];
                    if(selectClause) {
                        while (response.hits.hits.length > 0) {
                            var data = response.hits.hits.shift().fields;
                            var dataObject = {};
                            for(var k in data) {
                                dataObject[k] = data[k][0];
                            }
                            result.push(self.getObjectFromDBObject(dataObject));
                        }
                    }
                    else {
                        while (response.hits.hits.length > 0) {
                            result.push(self.getObjectFromDBObject(response.hits.hits.shift()._source));
                        }
                    }
                    return resolve(result);
                });

            });
        };

        Model.prototype.pagedFind = function pagedFind(criteria, selectClause, skip, limit){
            var self = this;
            return new Promise(function(resolve, reject) {

                var resource = {};
                if(criteria) {
                    resource = deleteAllPropertiesWhichAreUndefined(self.getDBObject(criteria));
                }
                var queryObject = {
                    index: self.tableName,
                    type: self.tableType,
                    from : skip || 0,
                    size : ((limit | 0) <= 1001 ? limit : 1000)
                };

                prepareQueryObject(queryObject, resource);
                _es.search(queryObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    var result = [];
                    while (response.hits.hits.length > 0) {
                        result.push(self.getObjectFromDBObject(response.hits.hits.shift()._source));
                    }
                    return resolve(result);
                });

            });
        };

        Model.prototype.save = function save(){
            var self = this;
            return saveData(self, self.input);
        };

        Model.prototype.getInBatch = function getInBatch() {
            var self = this;

            if(!self.input){
                return new Promise(function(resolve){
                    return resolve(null);
                });
            }

            return new Promise(function(resolve, reject) {

                var queryObject = {
                    index: self.tableName,
                    type: self.tableType,
                    body: {
                        ids: self.input
                    }
                };
                _es.mget(queryObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    var result = [];
                    while (response.docs.length > 0) {
                        var docObject = response.docs.shift();
                        if(docObject.found === true) {
                            result.push(self.getObjectFromDBObject(docObject._source));
                        }
                    }
                    return resolve(result);
                });
            });
        };

        Model.prototype.saveInBatch = function saveInBatch() {
            var self = this;

            var promises = [];

            for (var index = 0; index < self.input.length; index++) {
                promises.push(saveData(self, self.input[index]));
            }

            return Promise.all(promises).spread(function() {
                return true;
            });

        };

        Model.prototype.update = function update(whereClause, setFields) {
            var self = this;

            if (!whereClause || !whereClause.id) {
                throw new Error("id must be defined");
            }

            if(!setFields) {
                setFields = {};
            }

            return new Promise(function(resolve, reject) {

                var resource = deleteAllPropertiesWhichAreUndefined(self.getDBObject(setFields));
                var updateObject = {
                    index: self.tableName,
                    type: self.tableType,
                    id: whereClause.id,
                    body: prepareUpdateQueryObject(resource)
                };

                _es.update(updateObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    return resolve(response._id);
                });

            });
        };

        Model.prototype.deleteAll = function deleteAll() {
            var self = this;
            return new Promise(function(resolve, reject) {
                var queryObject = {
                    index: self.tableName,
                    type: self.tableType,
                    body: {query: {match_all: {}}}
                };
                _es.delete(queryObject, function(error, response) {
                    if(error) {
                        return reject(error);
                    }
                    return resolve(response);
                });
            });
        };

        Model.prototype.deleteObject = function deleteObject() {
            var self = this;
            return new Promise(function(resolve, reject) {

                var resource = {};
                if(self.input) {
                    resource = deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input));
                }
                var queryObject = {
                    index: self.tableName,
                    type: self.tableType
                };

                if (resource.id) {
                    queryObject.id = resource.id;
                    _es.delete(queryObject, function(error, response) {
                        prepareRawResponse(self, error, response, resolve, reject);
                    });
                }
                else {
                    prepareQueryObject(queryObject, resource);
                    _es.deleteByQuery(queryObject, function(error, response) {
                        prepareRawResponse(self, error, response, resolve, reject);
                    });
                }
            });
        };

        Model.prototype.getDBObject = function getDBObject(obj) {
            return obj;
        };

        Model.prototype.getObjectFromDBObject = function getObjectFromDBObject(obj) {
            return obj;
        };

        return Model;

    }

    return {
        extend: extend,
        client: _es
    };

}

module.exports.initialize = initialize;