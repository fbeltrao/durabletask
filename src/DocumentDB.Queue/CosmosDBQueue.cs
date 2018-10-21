using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Reader;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDB.Queue
{

    public class CosmosDBQueue
    {
        private readonly IChangeFeedReader changeFeedReader;
        DocumentClient documentClient;
        private readonly string databaseId;
        private readonly Uri collectionLink;
        private readonly string collectionName;

        public bool DeleteDocumentOnComplete { get; set; } = false;

        public CosmosDBQueue()
        {
        }

        public CosmosDBQueue(IChangeFeedReader changeFeedReader, DocumentClient documentClient, Microsoft.Azure.Documents.ChangeFeedProcessor.DocumentCollectionInfo documentCollectionInfo)
        {
            this.changeFeedReader = changeFeedReader;
            this.documentClient = documentClient;
            this.databaseId = documentCollectionInfo.DatabaseName;
            this.collectionName = documentCollectionInfo.CollectionName;
            this.collectionLink = UriFactory.CreateDocumentCollectionUri(this.databaseId, this.collectionName);
        }
        public async Task<IReadOnlyList<CosmosDBQueueMessage>> Dequeue()
        {
            var docs = await changeFeedReader.ReadAsync().ConfigureAwait(false);
            var completer = new CosmosDBQueueMessageCompleter(docs);

            var result = new List<CosmosDBQueueMessage>();
            foreach (var doc in docs.Docs)
            {
                result.Add(new CosmosDBQueueMessage(doc, completer));
            }

            return result;
        }

        public async Task<Document> Enqueue(object messageData)
        {
            var response = await documentClient.CreateDocumentAsync(this.collectionLink.ToString(), messageData).ConfigureAwait(false);
            return response.Resource;
        }

        public async Task Complete(CosmosDBQueueMessage cosmosDBQueueMessage)
        {
            await cosmosDBQueueMessage.Complete().ConfigureAwait(false);

            // TODO: remove document?
            if (this.DeleteDocumentOnComplete)
            {
                await documentClient.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(this.databaseId, this.collectionName, cosmosDBQueueMessage.Data.Id),
                    new RequestOptions
                    {
                        AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                        {
                            Condition = cosmosDBQueueMessage.Data.ETag,
                            Type = AccessConditionType.IfMatch
                        }
                    }).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Completes a message by identifier
        /// </summary>
        /// <param name="messageId"></param>
        /// <returns></returns>
        public async Task Complete(string messageId)
        {
            // TODO: remove document?
            if (this.DeleteDocumentOnComplete)
            {
                await documentClient.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.databaseId, this.collectionName, messageId)).ConfigureAwait(false);
            }
        }

        public async Task Abandon(CosmosDBQueueMessage cosmosDBQueueMessage)
        {
            // Option 1: just don't forward the reader, and reset the continuation value.
            // The problem lies in having other messages that were already dequeued, which saved the cursor ahead

            // Option 2: save document change, making it appear a second time
            var dequeueCount = cosmosDBQueueMessage.Data.GetPropertyValue<int?>("dequeueCount");
            cosmosDBQueueMessage.Data.SetPropertyValue("dequeueCount", 1 + (dequeueCount ?? 0));

            await documentClient.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(this.databaseId, this.collectionName, cosmosDBQueueMessage.Data.Id), cosmosDBQueueMessage.Data,
                new RequestOptions
                {
                    AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                    {
                        Condition = cosmosDBQueueMessage.Data.ETag,
                        Type = AccessConditionType.IfMatch
                    }
                }).ConfigureAwait(false);
        }       
    }
}
