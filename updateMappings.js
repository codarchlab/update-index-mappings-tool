var indices = require('./indices');

var args = process.argv.slice(2);
if (args=='init') {
    indices.initIfNotInitialized(function(err,res){
        if (err) {
            console.log(err);
        }
        if (res) {
            console.log(res);
        }
    });
}
else {
    indices.updateIndexMappings(function(err,res){
        if (err) {
            console.log(err);
        }
        if (res) {
            console.log(res);
        }
    });
}



