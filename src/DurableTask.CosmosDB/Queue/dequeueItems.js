/*
    @batchSize: the amount of items to dequeue
    @visibilityStarts: visibility starting datetime
    @lockDurationInSeconds: how long a dequeue item should be locked for
    @queueName: queue name to dequeue from
*/
function dequeueItems(batchSize, visibilityStarts, lockDurationInSeconds, queueName) {
    var collection = getContext().getCollection();

    var currentDate = Math.floor(new Date() / 1000);
    var dequeuedItemLockUntil = currentDate + lockDurationInSeconds;
    var searchItemsLockUntil = Math.floor(currentDate + (lockDurationInSeconds / 2));
    var itemsToReturn = [];
    var foundItemsCount = 0;
    var processedItemsCount = 0;

    var query = {
        query: 'SELECT TOP ' + batchSize + ' * FROM c WHERE c.NextVisibleTime <= @visibilityStarts AND c.QueueName = @queueName AND ((c.Status = "Pending") OR (c.Status="InProgress" AND c.LockedUntil > @searchItemsLockUntil)) ORDER by c.NextVisibleTime',
        parameters: [
            { name: "@queueName", value: queueName },
            { name: "@visibilityStarts", value: visibilityStarts },
            { name: '@searchItemsLockUntil', value: searchItemsLockUntil }]
    };

    collection.queryDocuments(
        collection.getSelfLink(),
        query,
        function (err, feed, options) {
            if (err) throw err;

            if (!feed || !feed.length) {
                var response = getContext().getResponse();
                /*
                var emptyResponse = {
                    items: [],
                    query: query,
                    batchSize: batchSize,
                    currentDate: currentDate,
                    dequeuedItemLockUntil: dequeuedItemLockUntil,
                    searchItemsLockUntil, searchItemsLockUntil
                };

                response.setBody(JSON.stringify(emptyResponse));*/
                response.setBody('[]');
                return response;
            }

            foundItemsCount = feed.length;

            updateDocument(feed, 0);

        }
    );

    function updateDocument(docs, index) {
        var doc = docs[index];
        doc.Status = 'InProgress';
        doc.DequeueCount = doc.DequeueCount + 1;
        doc.LockedUntil = dequeuedItemLockUntil;

        collection.replaceDocument(
            doc._self,
            doc,
            function (err, docReplaced) {
                if (!err) {
                    itemsToReturn.push(docReplaced);
                }

                ++processedItemsCount;

                if (processedItemsCount === foundItemsCount) {
                    var response = getContext().getResponse();
                    response.setBody(JSON.stringify(itemsToReturn));
                } else {
                    updateDocument(docs, index + 1);
                }
            }
        );
    }
}
