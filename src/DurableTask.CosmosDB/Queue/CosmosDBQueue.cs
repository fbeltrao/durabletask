using DurableTask.AzureStorage;
using DurableTask.Core;
using DurableTask.CosmosDB.Queue;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB
{
    /// <summary>
    /// Cosmos db queue
    /// </summary>
    public class CosmosDBQueue : IDisposable, IQueue
    {
        private CosmosDBQueueSettings settings;
        DocumentClient documentClientWithSerializationSettings;
        DocumentClient documentClientWithoutSerializationSettings;
        bool initialized = false;
        private readonly string queueName;
        const string PeekItemsStoredProcedureName = "peekItemsV2";


        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueue(CosmosDBQueueSettings settings, string name)
        {
            this.settings = settings;
            this.queueName = name;
        }


        /// <summary>
        /// Enqueues a message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        internal async Task<CosmosDBQueueMessage> Enqueue(CosmosDBQueueMessage message)
        {
            if (!this.initialized)            
                throw new InvalidOperationException("Must call Initialize() first");

            message.queueName = this.queueName;

            var uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);
            var res = await this.documentClientWithSerializationSettings.UpsertDocumentAsync(uri, message);
            message.etag = res.Resource.ETag;
            message.Id = res.Resource.Id;
            message.InsertionTime = res.Resource.Timestamp.ToUniversalTime();
            

            return message;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            StoredProcedureResponse<string> response = null;
            try
            {
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, PeekItemsStoredProcedureName);
                var queueVisibilityParameterValue = Utils.ToUnixTime(DateTime.UtcNow.Add(controlQueueVisibilityTimeout));
                response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    controlQueueBatchSize,
                    queueVisibilityParameterValue,
                    this.queueName);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    var result = JsonConvert.DeserializeObject<IEnumerable<CosmosDBQueueMessage>>(response.Response,
                        new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.Objects
                        });

                    if (result.Any())
                    {
                        var docs = (JArray)JsonConvert.DeserializeObject(response.Response);
                        for (var i = docs.Count-1; i >= 0; --i)
                        {
                            result.ElementAt(i).etag = docs[i]["_etag"].ToString();
                        }
                    }

                    return result;
                }

                return null;

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //throw;
            }

            return new CosmosDBQueueMessage[0];
        }


        internal async Task<IQueueMessage> GetMessageAsync(TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            StoredProcedureResponse<string> response = null;
            try
            {
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, PeekItemsStoredProcedureName);
                var queueVisibilityParameterValue = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(queueVisibilityTimeout));
                response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    1, 
                    queueVisibilityParameterValue,
                    this.queueName);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    var result = JsonConvert.DeserializeObject<IEnumerable<CosmosDBQueueMessage>>(response.Response,
                        new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.Objects
                        });


                    if (result.Any())
                    {
                        var docs = (JArray)JsonConvert.DeserializeObject(response.Response);
                        for (var i = docs.Count-1; i >= 0; --i)
                        {
                            result.ElementAt(i).etag = docs[i]["_etag"].ToString();
                        }
                    }

                    return result?.FirstOrDefault();
                }

                return null;

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //throw;
            }

            return null;
        }

        internal async Task CompleteAsync(string id)
        {
            var documentUri = UriFactory.CreateDocumentUri(
                settings.QueueCollectionDefinition.DbName,
                settings.QueueCollectionDefinition.CollectionName,
                id);

            await this.documentClientWithSerializationSettings.DeleteDocumentAsync(documentUri);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        

        /// <inheritdoc />
        public string Name => this.queueName;

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.documentClientWithSerializationSettings?.Dispose();
                    this.documentClientWithSerializationSettings = null;
                }

                disposedValue = true;
            }
        }
        

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {           
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            if (this.initialized)
                return;

            await Utils.CreateCollectionIfNotExists(this.settings.QueueCollectionDefinition);                       
            
            this.documentClientWithSerializationSettings = new DocumentClient(
                new Uri(this.settings.QueueCollectionDefinition.Endpoint),
                this.settings.QueueCollectionDefinition.SecretKey,
                new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.All
                });

            this.documentClientWithoutSerializationSettings = new DocumentClient(
               new Uri(this.settings.QueueCollectionDefinition.Endpoint),
               this.settings.QueueCollectionDefinition.SecretKey);

            var createStoredProcedure = false;
            try
            {
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, PeekItemsStoredProcedureName);
                await this.documentClientWithSerializationSettings.ReadStoredProcedureAsync(storedProcedureUri);

            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    createStoredProcedure = true;
            }

            if (createStoredProcedure)
            {
                var collectionUri = UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName);
                var storedProcedure = new StoredProcedure()
                {
                    Id = PeekItemsStoredProcedureName,
                    Body = @"
function peekItems(batchSize, visibilityStarts, queueName) {
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();

    // The count of imported docs, also used as current doc index.
    var count = 0;
    var itemsToReturn = [];
    var query = {
        query: 'SELECT TOP ' + batchSize + ' * FROM c WHERE c.status=""Pending"" AND c.queueName = @queueName',
        parameters: [{ name: ""@queueName"", value: queueName }]
    };


    collection.queryDocuments(
        collection.getSelfLink(),
        query,
        function (err, feed, options) {
            if (err) throw err;

            // Check the feed and if empty, set the body to 'no docs found', 
            // else take 1st element from feed
            if (!feed || !feed.length) {
                var response = getContext().getResponse();
                response.setBody('[]');
                return response;
            }
            else {
                count = feed.length;
                updateDocument(feed, 0);
            }
        });

    function updateDocument(docs, index) {
        var doc = docs[index];
        doc.status = 'InProgress';
        doc.lockedUntil = '';
        doc.dequeueCount = doc.dequeueCount + 1;

        collection.replaceDocument(
            doc._self,
            doc,
            function (err, docReplaced) {
                if (!err) {
                    itemsToReturn.push(docReplaced);
                }

                index++;
                if (index == count) {
                    var response = getContext().getResponse();
                    response.setBody(JSON.stringify(itemsToReturn));
                    return;
                } else {
                    updateDocument(docs, index);
                }
            });
    }
}",
                };

                var createStoredProcedureResponse = await this.documentClientWithSerializationSettings.CreateStoredProcedureAsync(collectionUri, storedProcedure);
            }

            this.initialized = true;
        }

        /// <inheritdoc />
        public Task<bool> DeleteIfExistsAsync()
        {
            // TODO: delete collection
            return Task.FromResult<bool>(true);
        }

       
        /// <inheritdoc />
        public async Task DeleteMessageAsync(IQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var cosmosQueueMessage = (CosmosDBQueueMessage)queueMessage;
            await this.documentClientWithSerializationSettings.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, cosmosQueueMessage.Id),
                new RequestOptions
                {
                    AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                    {
                        Condition = cosmosQueueMessage.etag,
                        Type = AccessConditionType.IfMatch
                    }
                });            
        }


        /// <inheritdoc />
        public Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields visibility, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            // TODO: implement it
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<IQueueMessage> PeekMessageAsync()
        {
            // TODO: implement it
            var qm = new CosmosDBQueueMessage();
            return Task.FromResult<IQueueMessage>(qm);
            //throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<int> GetQueueLenghtAsync()
        {
            //var query = "Select count(1) from c WHERE c.status = 'Pending'";
            // TODO: implement it

            var count = this.documentClientWithSerializationSettings.CreateDocumentQuery<CosmosDBQueueMessage>(
                UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName))
                .Where(x => x.status == QueueItemStatus.Pending)
                .Count();

            return Task.FromResult<int>(count);
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync()
        {
            // TODO: implement it
            return Task.FromResult<bool>(true);
        }
        #endregion
    }
}
