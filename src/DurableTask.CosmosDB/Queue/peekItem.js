
function peekItem() {
    var  collection = getContext().getCollection();

    // Query documents and take 1st item.
    var  isAccepted = collection.queryDocuments(
        collection.getSelfLink(),
        'SELECT top 1 * FROM c where c.status = "Pending" order by c.queuedTime asc',
        function  (err, feed, options) {
            if  (err)  throw  err;

            // Check the feed and if empty, set the body to 'no docs found', 
            // else take 1st element from feed
            if  (!feed || !feed.length) {
                var  response = getContext().getResponse();
                response.setBody('');
            } else  {
                var response = getContext().getResponse();
                var queueItem = feed[0];
                queueItem.status = 'InProgress';
                queueItem.lockedUntil = '';

                isAccepted = collection.replaceDocument(
                    queueItem._self,
                    queueItem,
                    function (err, docReplaced) {
                        if (err) throw "Unable to update queue item, abort ";
                        response.setBody(JSON.stringify(queueItem));
                    }
                );
            }
        });

    if  (!isAccepted)  throw  new  Error('The query was not accepted by the server.');
}