﻿using DurableTask.AzureStorage;
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
using System.IO;
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
        private readonly bool queueIsPartitioned;
        private CosmosDBQueueSettings settings;
        DocumentClient documentClientWithSerializationSettings;
        DocumentClient documentClientWithoutSerializationSettings;
        bool initialized = false;
        private readonly string queueName;
        //const int MaxTaskMessageLength = 200000;

        /// <summary>
        /// Indicate if completed queue items should be deleted
        /// </summary>
        public bool DeletedCompletedItems { get; set; } = true;

        /// <summary>
        /// Biggest size of message to be handled inline
        /// </summary>
        public int MaxTaskMessageLength { get; set; } = 0;

        const string TaskMessageAttachmentId = "taskMessage";
        const string DequeueItemsStoredProcedureName = "dequeueItems";
        const string DequeueItemsByPartitionStoredProcedureName = "dequeueItemsByPartition";

        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueue(CosmosDBQueueSettings settings, string name)
        {
            this.queueIsPartitioned = settings.QueueCollectionDefinition.PartitionKeyPaths?.Count > 0;
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

            message.QueueName = this.queueName;
            message.CreatedDate = Utils.ToUnixTime(DateTime.UtcNow);
            if (message.NextVisibleTime <= 0)
                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow);
            var uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);

            var res = await SaveQueueItem(message);

            message.ETag = res.Resource.ETag;
            message.Id = res.Resource.Id;            

            return message;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            StoredProcedureResponse<string> response = null;
            try
            {
                controlQueueBatchSize = Math.Max(controlQueueBatchSize, 20);
                var storedProceduredName = queueIsPartitioned ? DequeueItemsByPartitionStoredProcedureName : DequeueItemsStoredProcedureName;
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, storedProceduredName);
                var queueVisibilityParameterValue = Utils.ToUnixTime(DateTime.UtcNow.Add(controlQueueVisibilityTimeout));

                RequestOptions cosmosDbRequestOptions = null;
                if (queueIsPartitioned)
                {
                    cosmosDbRequestOptions = new RequestOptions
                    {
                        PartitionKey = new PartitionKey(this.queueName)
                    };
                }

                response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    cosmosDbRequestOptions,
                    controlQueueBatchSize,
                    queueVisibilityParameterValue,
                    this.QueueItemLockInSeconds,
                    this.queueName);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    return await BuildQueueItems(response.Response);
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
                var storedProcedureName = this.queueIsPartitioned ? DequeueItemsByPartitionStoredProcedureName : DequeueItemsStoredProcedureName;
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, storedProcedureName);
                var queueVisibilityParameterValue = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(queueVisibilityTimeout));


                RequestOptions cosmosDbRequestOptions = null;
                if (queueIsPartitioned)
                {
                    cosmosDbRequestOptions = new RequestOptions
                    {
                        PartitionKey = new PartitionKey(this.queueName)
                    };
                }

                response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    cosmosDbRequestOptions,
                    1, 
                    queueVisibilityParameterValue,
                    this.QueueItemLockInSeconds,
                    this.queueName);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    return (await BuildQueueItems(response.Response))?.FirstOrDefault();
                    
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

        private async Task<IEnumerable<IQueueMessage>> BuildQueueItems(string json)
        {
            var result = JsonConvert.DeserializeObject<IEnumerable<CosmosDBQueueMessage>>(json,
                        new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.Objects
                        });


            if (result.Any())
            {
                var docs = (JArray)JsonConvert.DeserializeObject(json);
                for (var i = docs.Count - 1; i >= 0; --i)
                {
                    var queueItem = result.ElementAt(i);
                    queueItem.ETag = docs[i]["_etag"].ToString();
                    
                    if (queueItem.Data is MessageData messageData)
                    {
                        messageData.OriginalQueueMessage = queueItem;
                        messageData.QueueName = queueItem.QueueName;
                        messageData.TotalMessageSizeBytes = 0;                                        

                        if (!string.IsNullOrEmpty(messageData.CompressedBlobName))
                        {
                            messageData.TaskMessage = await Utils.LoadAttachment<TaskMessage>(this.documentClientWithSerializationSettings, new Uri(messageData.CompressedBlobName, UriKind.Relative));                            
                        }
                    }
                }
            }

            return result;
        }

        internal async Task CompleteAsync(string id)
        {
            var documentUri = UriFactory.CreateDocumentUri(
                settings.QueueCollectionDefinition.DbName,
                settings.QueueCollectionDefinition.CollectionName,
                id);

            await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.DeleteDocumentAsync(documentUri));
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        

        /// <inheritdoc />
        public string Name => this.queueName;

        /// <summary>
        /// Queue item lock in seconds. Default is 60 seconds
        /// </summary>
        public int QueueItemLockInSeconds { get; set; } = 60;

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
                var storedProceduredName = this.queueIsPartitioned ? DequeueItemsByPartitionStoredProcedureName : DequeueItemsStoredProcedureName;
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, storedProceduredName);
                await this.documentClientWithSerializationSettings.ReadStoredProcedureAsync(storedProcedureUri);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    createStoredProcedure = true;
            }

            if (createStoredProcedure)
            {
                if (queueIsPartitioned)
                {
                    await CreateDequeueItemsByPartitionStoredProcedure();
                }
                else
                {
                    await CreateDequeueItemsStoredProcedure();
                }
                
            }

            this.initialized = true;
        }

        async Task CreateDequeueItemsByPartitionStoredProcedure()
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName);
            var storedProcedure = new StoredProcedure()
            {
                Id = DequeueItemsByPartitionStoredProcedureName,
                Body = @"
/*
    @batchSize: the amount of items to dequeue
    @visibilityStarts: visibility starting datetime
    @timeoutStarts: when the timeout for items in process starts
    @lockUntil: amount of time the items will be locked by
    @queueName: queue name to dequeue from - unnused since we rely on partitions
*/
function dequeueItemsByPartition(batchSize, visibilityStarts, lockDurationInSeconds, queueName) {
    var collection = getContext().getCollection();

    var currentDate = Math.floor(new Date() / 1000);
    var dequeuedItemLockUntil = currentDate + lockDurationInSeconds;
    var searchItemsLockUntil = Math.floor(currentDate + (lockDurationInSeconds / 2));
    var itemsToReturn = [];
    var foundItemsCount = 0;
    var processedItemsCount = 0;
    
    var query = {
        query: 'SELECT TOP ' + batchSize + ' * FROM c WHERE c.NextVisibleTime <= @visibilityStarts AND ((c.Status = ""InProgress"" AND @lockedLimit > c.LockedUntil) OR c.Status = ""Pending"") ORDER by c.NextVisibleTime',
        parameters: [
            { name: ""@visibilityStarts"", value: visibilityStarts }, 
            { name: ""@lockedLimit"", value: searchItemsLockUntil }]
    };

    collection.queryDocuments(
        collection.getSelfLink(),
        query,
        function (err, feed, options) {
            if (err) throw err;

            if (!feed || !feed.length) {
                var response = getContext().getResponse();                
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

                if (processedItemsCount == foundItemsCount) {
                    var response = getContext().getResponse();
                    response.setBody(JSON.stringify(itemsToReturn));
                } else {
                    updateDocument(docs, index + 1);
                }
            }
        );
    }
}
",
            };

            var createStoredProcedureResponse = await this.documentClientWithSerializationSettings.CreateStoredProcedureAsync(collectionUri, storedProcedure);
        }


        async Task CreateDequeueItemsStoredProcedure()
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName);
            var storedProcedure = new StoredProcedure()
            {
                Id = DequeueItemsStoredProcedureName,
                Body = @"
/*
    @batchSize: the amount of items to dequeue
    @visibilityStarts: visibility starting datetime
    @timeoutStarts: when the timeout for items in process starts
    @lockUntil: amount of time the items will be locked by
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
        query: 'SELECT TOP ' + batchSize + ' * FROM c WHERE c.NextVisibleTime <= @visibilityStarts AND c.QueueName = @queueName AND ((c.Status = ""Pending"")) ORDER by c.NextVisibleTime',
        parameters: [
            { name: ""@queueName"", value: queueName }, 
            { name: ""@visibilityStarts"", value: visibilityStarts }, 
            { name: '@searchItemsLockUntil', value: searchItemsLockUntil }]
    };

    collection.queryDocuments(
        collection.getSelfLink(),
        query,
        function (err, feed, options) {
            if (err) throw err;

            if (!feed || !feed.length) {
                var response = getContext().getResponse();                
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

                if (processedItemsCount == foundItemsCount) {
                    var response = getContext().getResponse();
                    response.setBody(JSON.stringify(itemsToReturn));
                } else {
                    updateDocument(docs, index + 1);
                }
            }
        );
    }
}
",
            };

            var createStoredProcedureResponse = await this.documentClientWithSerializationSettings.CreateStoredProcedureAsync(collectionUri, storedProcedure);
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
            if (this.DeletedCompletedItems)
            {
                var cosmosDBRequestOptions = new RequestOptions
                {
                    AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                    {
                        Condition = cosmosQueueMessage.ETag,
                        Type = AccessConditionType.IfMatch
                    },
                };

                if (queueIsPartitioned)
                    cosmosDBRequestOptions.PartitionKey = new PartitionKey(this.queueName);

                await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, cosmosQueueMessage.Id),
                    cosmosDBRequestOptions));
            }
            else
            { 
                cosmosQueueMessage.Status = QueueItemStatus.Completed;
                cosmosQueueMessage.TimeToLive = (int)Utils.ToUnixTime(DateTime.UtcNow.AddHours(1));
                await SaveQueueItem(cosmosQueueMessage);
            }
        }

        /// <summary>
        /// Helper to save the queue item
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        async Task<ResourceResponse<Document>> SaveQueueItem(CosmosDBQueueMessage message)
        {
            var reqOptions = new RequestOptions()
            {
                AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition()
                {
                    Condition = message.ETag,
                    Type = AccessConditionType.IfMatch,                    
                },                
            };

            if (this.queueIsPartitioned)
                reqOptions.PartitionKey = new PartitionKey(message.QueueName);

#if INCLUDED_EXPERIMENTAL_ATTACHMENTS
            TaskMessage attachmentTaskMessage = null;
            var messageData = message.Data as MessageData;
            if (messageData != null)
            {
                if (MaxTaskMessageLength > 0)
                {
                    if (messageData.TaskMessage != null)
                    {
                        var serializedTaskMessage = JsonConvert.SerializeObject(messageData.TaskMessage);
                        if (serializedTaskMessage.Length > MaxTaskMessageLength)
                        {
                            attachmentTaskMessage = messageData.TaskMessage;
                            messageData.TaskMessage = null;
                            messageData.MessageFormat = MessageFormatFlags.StorageBlob;

                            if (string.IsNullOrEmpty(message.Id))
                            {
                                message.Id = Guid.NewGuid().ToString();
                            }

                            messageData.CompressedBlobName = UriFactory.CreateAttachmentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, message.Id, TaskMessageAttachmentId).ToString();
                            if (message.NextVisibleTime == 0)
                                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.AddSeconds(5));
                            else
                                message.NextVisibleTime += 5;
                        }
                    }
                }
            }
#endif

            var createDocumentResponse = await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName), message, reqOptions));

#if INCLUDED_EXPERIMENTAL_ATTACHMENTS
            if (attachmentTaskMessage != null)
            {
                var createdDocumentUri = UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, createDocumentResponse.Resource.Id);
                var attachmentResponse = await Utils.SaveAttachment(this.documentClientWithSerializationSettings, createdDocumentUri, attachmentTaskMessage, TaskMessageAttachmentId);

                // set the task message back in the instance
                messageData.TaskMessage = attachmentTaskMessage;
            }
#endif

            return createDocumentResponse;
        }


        /// <inheritdoc />
        public async Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields updateFields, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var cosmosDBQueueMessage = (CosmosDBQueueMessage)originalQueueMessage;
            if (updateFields == MessageUpdateFields.Visibility)
            {
                // We "abandon" the message by settings its visibility timeout to zero.
                // This allows it to be reprocessed on this node or another node.
                cosmosDBQueueMessage.Status = QueueItemStatus.Pending;
                cosmosDBQueueMessage.LockedUntil = 0;

                await SaveQueueItem(cosmosDBQueueMessage);
            }
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
            FeedOptions feedOptions = null;
            if (this.queueIsPartitioned)
                feedOptions.EnableCrossPartitionQuery = true;

            var count = this.documentClientWithSerializationSettings.CreateDocumentQuery<CosmosDBQueueMessage>(
                UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName))
                .Where(x => x.Status == QueueItemStatus.Pending && x.NextVisibleTime <= Utils.ToUnixTime(DateTime.UtcNow))
                .Count();

            return Task.FromResult<int>(count);
        }

        /// <inheritdoc />
        public async Task<bool> ExistsAsync()
        {            
            try
            {
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, DequeueItemsStoredProcedureName);
                await this.documentClientWithSerializationSettings.ReadStoredProcedureAsync(storedProcedureUri);

            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return false;

                throw;
            }
            
            return true;
        }
#endregion
    }
}
