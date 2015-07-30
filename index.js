// models will contain data dependent classes
var es = require("./lib/es"),
    mongo = require("./lib/mongo"),
    mysql = require ("./lib/mysql");

function initialize(params){

    if(!params){
        throw new Error ("params must be defined");
    }
    if(!params.client){
        throw new Error ("client property must be defined");
    }
    if(params.client === "es"){
        return es.initialize(params);
    }
    else if (params.client === "mongo"){
        return mongo.initialize(params);
    }
    else if (params.client === "mysql"){
        return mysql.initialize(params);
    }
    else{
        throw new Error("client must be \"es, mongo or mysql \"");
    }

}

module.exports.initialize = initialize;
