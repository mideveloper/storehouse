var Db = require("mongodb").Db,
    Server = require("mongodb").Server,
    Promise = require("bluebird"),
    CommonHelper = require("./common");

function getClient(server, pools, callback) {

    var poolkey = server.host + ":" + server.port;
    var pool = pools[poolkey];

    if (!pool) {
        var db = new Db(server.db, new Server(server.host, server.port, {
            auto_reconnect: true
        }), {
            native_parser: true,
            w: 1
        });
        db.open(function(err, mongoclient) {
            if (err) {
                callback(err);
                return;
            }
            if(server.user && server.pass) {
                db.authenticate(server.user, server.pass, function(err, results) {
                    if (err || !results) {
                        callback(err);
                        return;
                    }
                    pools[poolkey] = mongoclient;
                    callback(err, pools[poolkey]); //return Promise
                });
            } else {
                pools[poolkey] = mongoclient;
                callback(err, pools[poolkey]); //return Promise
            }
        });
    }
    else {
        callback(null, pool);
    }

}

function initialize(params) {

    var pools = [];
    var server = {
        host: params.host,
        port: params.port,
        db: params.db,
        user: params.user,
        pass: params.pass
    };

    // TODO: Mongo supports sharding and replica logic, we should compline this class to support sharding and read replica

    function extend(attributes) {

        if (!attributes.tableName) {
            throw new Error("tableName must be defined");
        }

        function saveData(context, input) {
            var obj = CommonHelper.deleteAllPropertiesWhichAreUndefined(context.preSave(context.getDBObject(input)));
            if(obj._id) {
                return context.rawUpdate({_id: obj._id}, { $set: obj}, {upsert: true});
            }
            else{
                return new Promise(function(resolve, reject) {
                    getClient(server, pools, function(err, client) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        client.collection(context.tableName).save(obj, function(err, output) {
                            if (err) {
                                return reject(err);
                            }
                            return resolve(output);
                        });
                    });
                });
            }
        }

        function getArrayQuery(context, data) {
            var query_obj = {};
            data = CommonHelper.deleteAllPropertiesWhichAreUndefined(context.getDBObject(data));

            for (var property in data) {
                if (data[property] instanceof Array) {
                    query_obj[property] = {
                        $each: data[property]
                    };
                }
                else {
                    query_obj[property] = data[property];
                }
            }

            return query_obj;
        }

        function Model(obj) {

            //if model creation is not with new keyword then return it with new
            if (!(this instanceof Model)) {
                return new Model(obj);
            }

            this.tableName = attributes.tableName;

            if (attributes.idAttribute) {
                obj._id = obj[attributes.idAttribute];
                obj[attributes.idAttribute] = undefined;
            }

            this.input = obj;
        }

        Model.prototype.getRawClient = function getRawClient() {
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(client);
                });
            });
        };

        Model.prototype.update = function update(where_clause, set_fields, config) {
            var self = this;
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }


                    if (config) {
                        client.collection(self.tableName).update(CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(where_clause)),
                        {
                            $set : CommonHelper.deleteAllPropertiesWhichAreUndefined(self.preUpdate(self.getDBObject(set_fields)))

                        }, config, function(err, output) {
                            if (err) {
                                reject(err);
                                return;
                            }
                            resolve(output);
                            return;
                        });

                    }
                    else {
                        client.collection(self.tableName).update(CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(where_clause)),
                        {
                            $set : CommonHelper.deleteAllPropertiesWhichAreUndefined(self.preUpdate(self.getDBObject(set_fields)))
                        }, function(err, output) {
                            if (err) {
                                reject(err);
                                return;
                            }
                            resolve(output);
                            return;
                        });
                    }
                });
            });
        };

        Model.prototype.rawUpdate = function rawUpdate(where_clause, set_fields, config) {
            var self = this;
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }


                    if (config) {
                        client.collection(self.tableName).update(where_clause,
                        set_fields, config, function(err, output) {
                            if (err) {
                                reject(err);
                                return;
                            }
                            resolve(output);
                            return;
                        });

                    }
                    else {
                        client.collection(self.tableName).update(where_clause,
                        set_fields, function(err, output) {
                            if (err) {
                                reject(err);
                                return;
                            }
                            resolve(output);
                            return;
                        });
                    }
                });
            });
        };

        Model.prototype.save = function save() {
            var self = this;
            return saveData(self, self.input);

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

        function prepareSelectClause(arg) {
            var select_clause_obj = {};

            if (!arg) {
                return select_clause_obj;
            }
            for (var i = 0; i < arg.length; i++) {
                select_clause_obj[arg[i]] = "1";
            }
            return select_clause_obj;
        }

        Model.prototype.fetch = function fetch() {
            var self = this;

            var select_clause = prepareSelectClause(arguments);

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    //  deleting all properties which are undefined because otherwise mongo will make them part of query that results
                    //  inappropriate data
                    var mongo_obj = CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(self.input));

                    client.collection(self.tableName).find(
                    mongo_obj, select_clause, {
                        limit: 1
                    }).toArray(function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }

                        if (output.length > 0) {
                            resolve(self.getObjectFromDBObject(output.shift()));
                            return;
                        }
                        else {
                            resolve(null);
                            return;
                        }
                    });
                });
            });
        };

        Model.prototype.getInBatch = function getInBatch() {
            var self = this;

            if(!self.input){
                return new Promise(function(resolve){
                    return resolve(null);
                });
            }

            var select_clause = prepareSelectClause(arguments);

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }


                    client.collection(self.tableName).find({
                        _id: {
                            $in: self.input
                        }
                    }, select_clause, {
                        limit: self.input.length
                    }).toArray(function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }

                        var result = [];

                        while (output.length > 0) {
                            result.push(self.getObjectFromDBObject(output.shift()));
                        }
                        resolve(result);
                        return;
                    });
                });
            });
        };

        Model.prototype.find = function find(criteria, select_clause, projection) {
            var self = this;
            select_clause = prepareSelectClause(select_clause);
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }


                    client.collection(self.tableName).find(criteria, select_clause, projection).toArray(function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        var result = [];

                        while (output.length > 0) {
                            result.push(self.getObjectFromDBObject(output.shift()));
                        }
                        resolve(result);
                        return;
                    });
                });
            });
        };

        Model.prototype.pagedFind = function pagedFind(criteria, select_clause, skip, limit, projection){

            var self = this;
            select_clause = prepareSelectClause(select_clause);
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    client.collection(self.tableName).find(criteria, select_clause, projection).skip(skip).limit(limit).toArray(function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        var result = [];

                        while (output.length > 0) {
                            result.push(self.getObjectFromDBObject(output.shift()));
                        }
                        resolve(result);
                        return;
                    });
                });
            });

        };

        Model.prototype.getCount = function getCount(criteria, select_clause, projection) {
            var self = this;
            select_clause = prepareSelectClause(select_clause);
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    //using default value of applySkipLimit
                    client.collection(self.tableName).find(criteria, select_clause, projection).count("", function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.appendArrayItemsIfNotExist = function appendArrayItemsIfNotExist(where_clause) {
            var self = this;

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    client.collection(self.tableName).update(
                    CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(where_clause)), {
                        $addToSet: getArrayQuery(self, self.input)
                    }, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.appendArrayItems = function appednArrayItems(where_clause) {
            var self = this;

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    client.collection(self.tableName).update(
                    CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(where_clause)), {
                        $push: getArrayQuery(self, self.input)
                    }, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.getDBObject = function getDBObject(obj) {
            return obj;
        };

        Model.prototype.getObjectFromDBObject = function getObjectFromDBObject(obj) {
            return obj;
        };

        Model.prototype.preSave = function preSave(obj) {
            return obj;
        };

        Model.prototype.preUpdate = function preUpdate(obj) {
            return obj;
        };

        Model.prototype.preDelete = function preDelete(obj) {
            return obj;
        };

        Model.prototype.deleteAll = function deleteAll() {
            var self = this;
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    client.collection(self.tableName).remove(err, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });

                });
            });
        };

        Model.prototype.deleteObject = function deleteObject() {
            var self = this;
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    var mongo_obj = CommonHelper.deleteAllPropertiesWhichAreUndefined(self.preDelete(self.getDBObject(self.input)));

                    client.collection(self.tableName).remove(mongo_obj, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.removeArrayItems = function removeArrayItems(where_clause) {
            var self = this;
            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    client.collection(self.tableName).update(
                    CommonHelper.deleteAllPropertiesWhichAreUndefined(self.getDBObject(where_clause)), {
                        $pull: getArrayQuery(self, self.input)
                    }, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.updateRow = function updateRow(where_clause, set_fields, is_upsert, is_multi) {
            var self = this;

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    client.collection(self.tableName).update(
                    where_clause,
                    set_fields, {
                        upsert: is_upsert,
                        multi: is_multi
                    }, function(err, output) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(output);
                        return;
                    });
                });
            });
        };

        Model.prototype.findAndModifyRow = function findAndModifyRow(
        query, sort, remove, update, new_data, fields, upsert) {
            var self = this;

            return new Promise(function(resolve, reject) {
                getClient(server, pools, function(err, client) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    client.collection(self.tableName).findAndModifyRow({
                        query: query
                    }, {
                        sort: sort
                    }, {
                        remove: remove
                    }, {
                        update: update
                    }, {
                        new: new_data
                    }, {
                        fields: fields
                    }, {
                        upsert: upsert
                    }, function(err, output) {
                        if (err) {
                            reject(err);
                        }
                        resolve(output);
                    });

                });
            });

        };

        return Model;
    }

    return {
        extend: extend

    };
}

module.exports.initialize = initialize;
