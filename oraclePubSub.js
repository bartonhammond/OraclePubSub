var redis = require("redis");

//Pair for storedProc
var storedProcSubscribe = redis.createClient();
var storedProcPublish = redis.createClient();

//Pair for query
var querySubscribe = redis.createClient();
var queryPublish = redis.createClient();

//JDBC sql data types
var sqlType =  {};
sqlType.BIGINT = -5;
sqlType.BOOLEAN = 16;
sqlType.BLOB = 2004;
sqlType.DOUBLE = 8;
sqlType.FLOAT = 6;
sqlType.INTEGER = 4;
sqlType.NVARCHAR = -9;
sqlType.VARCHAR = 12;
sqlType.TINYINT = -6;
sqlType.SMALLINT = 5;
sqlType.TIMESTAMP = 93;

//Make available
exports.sqlType = sqlType;

redis.debug_mode = false;
exports.setup = function () {
   storedProcSubscribe.on("ready", function () {
       storedProcSubscribe.subscribe("storedProc-output");
    });
   querySubscribe.on("ready", function () {
       querySubscribe.subscribe("query-output");
    });

 }
exports.performStoredProc = function(proc, cb) {
    storedProcSubscribe.on("message", function (channel, message) {
        cb(message);
    });
    storedProcPublish.publish("storedProc",proc);
};

exports.performQuery = function(query, cb) {
    querySubscribe.on("message", function (channel, message) {
        cb(message);
    });
    
    queryPublish.publish("query",query);
};

//Client call to end the listening
exports.end = function end() {
    querySubscribe.end();
    queryPublish.end();
    storedProcSubscribe.end();
    storedProcPublish.end();

}

