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

/**
 * Update mappings for all types
 *
 * @param index
 * @param callback
 */
function updateMappings(index, callback) {
    var promises = [];
    types.forEach(function(type) {

        console.log("type: "+type+" updated ");
            // ":"+JSON.stringify(mappings[type]))

        promises.push(client.indices.putMapping({
            index: index, type: type, body: mappings[type]
        }));
    });
    Promise.all(promises).then(function(res) {
        callback(null, res);
    }).catch(function(err) {
        callback(err, null)
    });
}


/**
 * Reindex current index documents to new index
 *
 * @param sourceIndex
 * @param targetIndex
 * @param lastIndexTime
 * @param callback
 */
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
        if (res.hits.total == 0) return callback(null, true);
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
}

/**
 * Get index to which alias currently points
 */
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
}

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

        performUpdate(indexName+ "_1",indexName,indexName,
            delAndPutAlias,
            function(err,alias){
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
 * @param newIndex
 * @param currentIndex
 * @param alias
 * @param callback
 */
var delAndPutAlias = function(newIndex,currentIndex,alias,callback) {
    client.indices.delete({index: alias}, function (err, res) {
        console.log("DEBUG - Deleted old index " + alias, res);
        client.indices.putAlias({index: newIndex, name: alias}, function (err, res) {
            //if (err) return callback(err, null);
            console.log("DEBUG - Added alias", res);
            return callback(null, alias);
        })
    });
};

/**
 * @param newIndex
 * @param currentIndex
 * @param alias
 * @param callback
 */
var switchAlias = function(newIndex,currentIndex,alias,callback) {
    client.indices.deleteAlias(
        { index: currentIndex, name: indexName}, function(err, res) {
            if (err) return callback(err, null);
            console.log("DEBUG - Deleted alias", res);
            client.indices.putAlias({ index: newIndex, name: indexName }, function(err, res) {
                if (err) return callback(err, null);
                console.log("DEBUG - Added alias", res);
                return callback(null, { success: true, currentIndex: currentIndex });
            });
        })
};

/**
 * @param newIndex name of the new index to create
 * @param currentIndex name of the current index
 * @param alias alias which should point to newIndex after successful operation.
 * @param after function to perform after everything else is done in the common body.
 * @param callback
 */
var performUpdate = function(newIndex,currentIndex,alias,after,callback) {

    // delete new index (if it already exists)
    client.indices.delete({ index: newIndex }, function(err, res) {
        console.log("DEBUG - Deleted index " + newIndex);

        client.indices.create({index: newIndex}, function (err, res) {
            if (err) return callback(err, null);

            console.log("DEBUG - Created index " + newIndex);

            updateMappings(newIndex, function (err, res) {

                if (err) return callback(err, null);
                console.log("DEBUG - Updated mappings", res);

                copyIndex(currentIndex, newIndex, "1900-01-01", function (err, res) {
                    if (err) return callback(err, null);
                    console.log("DEBUG - Copied index " + alias + " to " + newIndex);

                    after(newIndex,currentIndex,alias,callback);
                });
            });
        });
    });
};


/**
 * Reindex the whole index by creating a new index with updated mappings,
 * copying the documents from the current index and setting the alias when done
 */
var updateIndexMappings = function(callback) {

    retrieveCurrentIndex(indexName, function(err, currentIndex) {
        if (err) return callback(err, null);

        // set new index accordingly
        var newIndex = indexName + "_2"
        if (currentIndex == indexName + "_2") {
            var newIndex = indexName + "_1";
        }

        console.log("Concrete indexName: "+currentIndex);
        performUpdate(
            newIndex,currentIndex,indexName,
            switchAlias,callback);
    });
};

module.exports = {
	updateIndexMappings: updateIndexMappings,
    initIfNotInitialized: determineIndexExistenceCondition
};