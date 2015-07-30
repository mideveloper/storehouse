var chai = require("chai"),
    chaiAsPromised = require("chai-as-promised"),
    Promise = require("bluebird"),
    expect = chai.expect;

chai.use(chaiAsPromised);

describe("MySql", function() {
    var mysql = require("../lib/mysql");
    var FunctionalTestModel, mysqlBaseModel;
    var idColumnName = "id",
        nameColumnName = "name";

    before(function() {
        mysqlBaseModel = mysql.initialize({
            host: "localhost",
            port: "3306",
            db: "testMySqlModel",
            uid: "dbuser",
            pwd: "dbuser"
        });
        console.log("init-MySql");
        FunctionalTestModel = mysqlBaseModel.extend({
            tableName: "test"
        });
    });

    after(function() {
        return new FunctionalTestModel().deleteAll().then(function() {
            return true;
        });
    });

    it("save", function(done) {
        var insert_obj = {};
        insert_obj[idColumnName] = 1;
        insert_obj[nameColumnName] = "ftest1";
        return new FunctionalTestModel(insert_obj).save().then(function() {
            expect(true).to.equal(true);
            done();
        });
    });
    
    it("find", function(done) {
        var insert_obj = {};
        insert_obj[idColumnName] = 1;
        insert_obj[nameColumnName] = "ftest1";
        insert_obj["search_column"] = "search me";
    
        var insert_obj2 = {};
        insert_obj2[idColumnName] = 2;
        insert_obj2[nameColumnName] = "ftest2";
        insert_obj2["search_column"] = "search me";
    
        var insert_obj3 = {};
        insert_obj3[idColumnName] = 3;
        insert_obj3[nameColumnName] = "ftest3";
        insert_obj3["search_column"] = "no search me 2";
    
        var insert_obj4 = {};
        insert_obj4[idColumnName] = 4;
        insert_obj4[nameColumnName] = "ftest4";
        insert_obj4["search_column"] = "no search me 2";
    
        return Promise.all([
            new FunctionalTestModel(insert_obj).save(),
            new FunctionalTestModel(insert_obj2).save(),
            new FunctionalTestModel(insert_obj3).save(),
            new FunctionalTestModel(insert_obj4).save()
        ]).spread(function() {
            new FunctionalTestModel().find({
                "search_column": "search me"
            }).then(function(data) {
                expect(data.length).to.equal(2);
                done();
            });
        });
    });

    it("pagedFind", function(done) {
        return new FunctionalTestModel().pagedFind({
            "search_column": "search me"
        }, undefined, 0, 1).then(function(data) {
            expect(data.length).to.equal(1);
            done();
        });
    });

    it("fetch", function(done) {
        var where_clause = {};
        where_clause[idColumnName] = 1;
        return new FunctionalTestModel(where_clause).fetch().then(function(output) {
            expect(output[idColumnName]).to.equal(1);
            expect(output[nameColumnName]).to.equal("ftest1");
            done();
        });
    });

    it("fetch with select clause one parameter", function(done) {
        var where_clause = {};
        where_clause[idColumnName] = 1;
        return new FunctionalTestModel(where_clause).fetch(idColumnName).then(function(output) {
            expect(output[idColumnName]).to.equal(1);
            expect(output[nameColumnName]).to.be.undefined;
            done();
        });
    });

    it("fetch with select clause multiple parameters", function(done) {
        var where_clause = {};
        where_clause[idColumnName] = 1;
        return new FunctionalTestModel(where_clause).fetch(idColumnName, nameColumnName).then(function(output) {
            expect(output[idColumnName]).to.equal(1);
            expect(output[nameColumnName]).to.equal("ftest1");
            done();
        });
    });
});
