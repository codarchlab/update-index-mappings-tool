/**
 * Provides an abstraction from the lower level elasticsearch indices API.
 *
 * Clients can
 * * test if an index exist
 * * create an index
 * * update the index mapping
 *
 *
 * @author Sebastian Cuy
 * @author Daniel M. de Oliveira
 */


var elasticsearch = require('elasticsearch');
var Promise = require('bluebird');
var types = require('./config/application').types;

var indexName = require('./config/application').indexName;
var esAddress = require('./config/application').esAddress

var mappings = {};
types.forEach(function(type) {
	var path = './config/types/' + type + '/mapping';
	mappings[type] = require(path);
});

var client = new elasticsearch.Client({
	host: esAddress,
	log: 'info'
});


// update mappings for all types
function updateMappings(index, callback) {
    var promises = [];
    types.forEach(function(type) {

        console.log("type:"+type+"content:"+JSON.stringify(mappings[type]))


        promises.push(client.indices.putMapping({
            index: index, type: type, body: mappings[type]
        }));
    });
    Promise.all(promises).then(function(res) {
        callback(null, res);
    }).catch(function(err) {
        callback(err, null)
    });
};



// reindex current index documents to new index
function copyIndex(sourceIndex, targetIndex, lastIndexTime, callback) {

    var currentIndexTime = new Date();

    var esReq = {
        index: sourceIndex,
        scroll: '1m',

        // TODO REVIEW - REPLACED THE QUERY
        body: {
            query: {
                match_all: {}
            }
        }
    };
    console.log("Scroll query", JSON.stringify(esReq, null, 4));
    var count = 0;
    client.search(esReq, function getMoreUntilDone(err, res) {
        if (err) return callback(err, null);
        console.log("Scrolling", res);
        if (res.hits.total == 0) return callback("0 hits.   Nothing to copy.", true);
        var bulk = [];
        res.hits.hits.forEach(function(hit) {
            bulk.push({ index:  { _index: targetIndex, _type: hit._type, _id: hit._id } });
            bulk.push(hit._source);
            count++;
        });
        client.bulk({ body: bulk }, function(err, bulkRes) {
            console.log("Bulk index result", bulkRes);
            if (err) return callback(err, null);
            if (res.hits.total > count) {
                client.scroll({ scrollId: res._scroll_id, scroll: '1m' }, getMoreUntilDone);
            } else {
                client.clearScroll({ scrollId: res._scroll_id });
                // repeform copy with new lastIndexTime to ensure new documents are copied

                // TODO REVIEW - THE NEXT LINE GOT REPLACED BY THE ONE FOLLOWING - SEE ALSO COMMENT ABOVE
                // copyIndex(sourceIndex, targetIndex, currentIndexTime, callback);
                return callback(null,true);
            }
        });
    });

};

// get index to which alias currently points
function retrieveCurrentIndex(alias, callback) {
    client.indices.getAlias({ name: alias }, function(err, res) {
        if (err) return callback(err, null);
        var indices = Object.keys(res);
        var currentIndex = indexName + "_1";
        if (indices.length == 1) {
            var currentIndex = indices[0];
        }
        callback(null, currentIndex);
    });
};

/**
 * Determines which of the items
 * indexName, indexName_1, indexName does or does not exist.
 *
 * @param callback
 */
var determineIndexExistenceCondition = function(callback){

    client.indices.get({index:indexName}, function(indexDoesNotExist) {
        client.indices.get({index:indexName+"_1"}, function(index_1DoesNotExist) {
            client.indices.get({index:indexName+"_2"}, function(index_2DoesNotExist) {
                handleIndexExistenceCondition(
                    indexDoesNotExist,
                    index_1DoesNotExist,
                    index_2DoesNotExist,
                    callback);
            });
        });
    });
};


/**
 * Handles the existence conditions determindes in
 * {@link determineIndexExistenceConditions}. Under certain
 * conditions (see implementation) the initial index update
 * operation gets performed.
 *
 * @param indexDoesNotExist
 * @param indexAlias_1DoesNotExist
 * @param indexAlias_2DoesNotExist
 * @param callback
 * @returns a callback (res,msg) with a msg describing if the update operation
 *   has been performed or why      if not.
 */
var handleIndexExistenceCondition= function(
    indexDoesNotExist,
    indexAlias_1DoesNotExist,
    indexAlias_2DoesNotExist,
    callback) {

    if (indexDoesNotExist) {
        return callback(null,indexName + " does not exist. \n" +
            "Neither as index name nor as alias. Aborting operation.");
    }

    if (indexAlias_2DoesNotExist&&indexAlias_1DoesNotExist) {

        initialIndexUpdate(indexName+ "_1",indexName,function(err,alias){
            if (err) {
                return callback(err,"Could not perform operation properly. " +
                    "The indices may be in an inconsistent state now.")
            } else {
                return callback(null,"The routine finished properly. The concrete index's name is " +
                    indexName+"_1 and the alias is "+alias+ " now.")
            }
        });
    } else {
        return callback(null,"If you want to perform the init routine, \n" +
            "one of the two following indices must exist, \n"+
            indexName+"_1 exists: "+!indexAlias_1DoesNotExist+"\n"+
            indexName+"_2 exists: "+!indexAlias_2DoesNotExist+"\n"+
            "Only the index named "+indexName+ " itself should be there.");
    }
};

/**
 * Creates an index named newIndexName and provides the newest mappings for it.
 * Then copies the contents the of the index named alias over to newIndexName and
 * deletes the index named alias. So the alias can be used AS alias for newIndexName
 * now.
 *
 * @param newIndexName the newly created concrete index.
 * @param alias the name of the old index which will get deleted and used as an alias for
 *   newIndexName afterwards.
 * @param callback
 */
var initialIndexUpdate = function(newIndexName,alias,callback) {

    client.indices.create({ index: newIndexName }, function(err, res) {
        if (err) return callback(err, null);
        console.log("Created index", newIndexName, res);
        updateMappings(newIndexName, function(err, res) {
            if (err) return callback(err, null);
            console.log("Updated mappings", res);
            copyIndex(alias, newIndexName, "1900-01-01", function(err, res) {
                if (err) return callback(err, null);
                console.log("Copied index "+ alias +" to "+ newIndexName);

                client.indices.delete({ index: alias }, function(err, res) {
                    console.log("Deleted old index "+ alias, res);
                    client.indices.putAlias({ index: newIndexName, name: alias }, function(err, res) {
                        //if (err) return callback(err, null);
                        console.log("Added alias", res);
                        return callback(null, alias);
                    })
                });
            });
        });
    });

};

// reindex the whole index by creating a new index with updated mappings,
// copying the documents from the current index and setting the alias when done
var updateIndexMappings = function(callback) {

    retrieveCurrentIndex(indexName, function(err, currentIndex) {
        if (err) return callback(err, null);

        // set new index accordingly
        var newIndex = indexName + "_2"
        if (currentIndex == indexName + "_2") {
            var newIndex = indexName + "_1";
        }

        console.log("Concrete indexName: "+currentIndex);


        // delete new index (if it already exists)
        client.indices.delete({ index: newIndex }, function(err, res) {
            console.log("Deleted index", newIndex, res);
            // create new index
            client.indices.create({ index: newIndex }, function(err, res) {
                if (err) return callback(err, null);
                console.log("Created index", newIndex, res);
                updateMappings(newIndex, function(err, res) {
                    if (err) return callback(err, null);
                    console.log("Updated mappings", res);
                    copyIndex(currentIndex, newIndex, "1900-01-01", function(err, res) {
                        if (err) return callback(err, null);
                        console.log("Copied index", currentIndex, newIndex);
                        client.indices.deleteAlias(
                                { index: currentIndex, name: indexName}, function(err, res) {
                            if (err) return callback(err, null);
                            console.log("Deleted alias", res);
                            client.indices.putAlias({ index: newIndex, name: indexName }, function(err, res) {
                                if (err) return callback(err, null);
                                console.log("Added alias", res);
                                return callback(null, { success: true, currentIndex: currentIndex });
                            });
                        })
                    });
                });
            });
        });

    });
};

module.exports = {
	updateIndexMappings: updateIndexMappings,
    initIfNotInitialized: determineIndexExistenceCondition
};