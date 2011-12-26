var oraclePubSub = require('./oraclePubSub');
var async = require('async');

//Setup the pub & sub
oraclePubSub.setup();

//Use async so that both can run in parallel
//and at conclusion the end can be invoked
//so script ends gracefully
async.parallel({

    //This query is same as stored proc so that testing was easier
    query: function(callback) {
        oraclePubSub.performQuery("select first_name, last_name, email,employee_id from HR.EMPLOYEES where department_id = 60 order by last_name, first_name asc",function(data){
            callback(null,data);
        });
    },
    storedProc: function(callback) {
        var foo = {};
        foo.sp = "BEGIN hr.get_emp_rs(?, ?); END;";
        //Do arg_types/args in parallel
        foo.arg_types = [oraclePubSub.sqlType.INTEGER];
        foo.args = [60];
        //What column contains the ResultSet
        foo.rs = 2;
        var fooStr = JSON.stringify(foo);
        oraclePubSub.performStoredProc(fooStr,function(data){
            callback(null,data);
        });
    },
},
               /**
                  * Results contains array 
                  */
               function(err,results) {
                   processQueryResults(results.query);
                   processStoredProc(results.storedProc);
                   oraclePubSub.end();
               });


function processQueryResults(queryResults){
    var qrArray = eval(queryResults);
    qrArray.forEach(function(employee) {
        console.log(employee);
    });
    
}
function processStoredProc(storedProcResults) {
    var sprArr = eval(storedProcResults);
    sprArr.forEach(function(employee) {
        console.log(employee);
    });
}