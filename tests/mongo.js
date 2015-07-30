var chai = require("chai"),
    chaiAsPromised = require("chai-as-promised"),
    expect = chai.expect,
    Promise = require("bluebird");
chai.use(chaiAsPromised);

describe("Mongo", function() {
    var mongo = require("../lib/mongo");
    var FunctionalTestModel, mongoBaseModel;
    before(function() {
        mongoBaseModel = mongo.initialize({
            host: "localhost",
            port: "27017",
            db: "functionaltest"
        });
        console.log("init-Mongo");
        FunctionalTestModel = mongoBaseModel.extend({
            tableName: "test"
        });
    });

    after(function() {
        return new FunctionalTestModel().deleteAll().then(function() {
            return true;
        });
    });

    it("save", function(done) {
        return new FunctionalTestModel({
            _id: 1,
            name: "ftest1"
        }).save().then(function() {
                expect(true).to.equal(true);
                done();
            });
    });

    it("find", function(done) {
        return Promise.all([
            new FunctionalTestModel({
                _id: 1,
                name: "ftest1",
                search_column: "search me"
            }).save(),
            new FunctionalTestModel({
                _id: 2,
                name: "ftest2",
                search_column: "search me"
            }).save(),
            new FunctionalTestModel({
                _id: 3,
                name: "ftest3",
                search_column: "no search me 2"
            }).save(),
            new FunctionalTestModel({
                _id: 4,
                name: "ftest4",
                search_column: "no search me 2"
            }).save()
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

    it("update", function(done) {
        return new FunctionalTestModel().update({
            _id: 1
        }, {
            name: "ftest2"
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                    expect(output._id).to.equal(1);
                    expect(output.name).to.equal("ftest2");
                    done();
                });
        });
    });

    it("rawUpdate", function(done) {
        return new FunctionalTestModel().rawUpdate({
            _id: 1
        }, {
            name: "ftest1"
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                    expect(output._id).to.equal(1);
                    expect(output.name).to.equal("ftest1");
                    done();
                });
        });
    });

    it("fetch", function(done) {
        return new FunctionalTestModel({
            _id: 1
        }).fetch().then(function(output) {
                expect(output._id).to.equal(1);
                expect(output.name).to.equal("ftest1");
                done();
            });
    });

    it("fetch with select clause one parameter", function(done) {
        return new FunctionalTestModel({
            _id: 1
        }).fetch("_id").then(function(output) {
                expect(output._id).to.equal(1);
                expect(output.name).to.be.undefined;
                done();
            });
    });

    it("fetch with select clause multiple parameters", function(done) {
        return new FunctionalTestModel({
            _id: 1
        }).fetch("_id", "name").then(function(output) {
                expect(output._id).to.equal(1);
                expect(output.name).to.equal("ftest1");
                done();
            });
    });

    it("append single item to array", function(done) {
        return new FunctionalTestModel({
            test_array: "1"
        }).appendArrayItems({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array[0]).to.equal("1");
                done();
            });
    });

    it("append multiple items to array", function(done) {
        return new FunctionalTestModel({
            test_array: ["2", "3"]
        }).appendArrayItems({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array[0]).to.equal("1");
                done();
            });
    });

    it("append single item to array if not exist (should add)", function(done) {
        return new FunctionalTestModel({
            test_array: ["4"]
        }).appendArrayItemsIfNotExist({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array.length).to.equal(4);
                done();
            });
    });

    it("append single item to array if not exist (should not add)", function(done) {
        return new FunctionalTestModel({
            test_array: ["4"]
        }).appendArrayItemsIfNotExist({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array.length).to.equal(4);
                done();
            });
    });

    it("append multiple items to array if not exist (should add)", function(done) {
        return new FunctionalTestModel({
            test_array: ["5", "6"]
        }).appendArrayItemsIfNotExist({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array.length).to.equal(6);
                done();
            });
    });

    it("append multiple items to array if not exist (should not add)", function(done) {
        return new FunctionalTestModel({
            test_array: ["5", "6"]
        }).appendArrayItemsIfNotExist({
            _id: 1
        }).then(function() {
            return new FunctionalTestModel({
                _id: 1
            }).fetch().then(function(output) {
                return output;
            });
        }).then(function(o) {
                expect(o.test_array.length).to.equal(6);
                done();
            });
    });
});
