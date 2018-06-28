//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using DurableTask.Core;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Cosmos db queue
    /// </summary>
    public class CosmosDBQueue : IDisposable, IQueue
    {
        private readonly CosmosDBQueueSettings settings;
        DocumentClient documentClientWithSerializationSettings;
        DocumentClient documentClientWithoutSerializationSettings;
        bool initialized = false;

        /// <summary>
        /// Indicate if completed queue items should be deleted
        /// </summary>
        public bool DeletedCompletedItems { get; set; } = true;

        /// <summary>
        /// Biggest size of message to be handled in-line
        /// </summary>
        public int MaxTaskMessageLength { get; set; } = 0;

        const string TaskMessageAttachmentId = "taskMessage";
        const string DequeueItemsStoredProcedureName = "dequeueItems";


        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueue(CosmosDBQueueSettings settings, string name)
        {
            this.settings = settings;
            this.Name = name;
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

            message.QueueName = this.Name;
            message.CreatedDate = Utils.ToUnixTime(DateTime.UtcNow);
            if (message.NextVisibleTime <= 0)
                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow);
            Uri uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);

            ResourceResponse<Document> res = await SaveQueueItem(message);

            message.ETag = res.Resource.ETag;
            message.Id = res.Resource.Id;

            return message;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            try
            {
                controlQueueBatchSize = Math.Min(controlQueueBatchSize, 20);

                Uri storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, DequeueItemsStoredProcedureName);
                long queueVisibilityParameterValue = Utils.ToUnixTime(DateTime.UtcNow.Add(controlQueueVisibilityTimeout));
                StoredProcedureResponse<string> response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    controlQueueBatchSize,
                    queueVisibilityParameterValue,
                    this.QueueItemLockInSeconds,
                    this.Name);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    return await this.BuildQueueItems(response.Response);
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
            try
            {
                Uri storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, DequeueItemsStoredProcedureName);
                int queueVisibilityParameterValue = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(queueVisibilityTimeout));
                StoredProcedureResponse<string> response = await this.documentClientWithoutSerializationSettings.ExecuteStoredProcedureAsync<string>(
                    storedProcedureUri,
                    1, 
                    queueVisibilityParameterValue,
                    this.QueueItemLockInSeconds,
                    this.Name);

                if (!string.IsNullOrEmpty(response.Response) && response.Response != "[]")
                {
                    return (await this.BuildQueueItems(response.Response))?.FirstOrDefault();
                    
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

            IEnumerable<CosmosDBQueueMessage> cosmosDbQueueMessages = result as IList<CosmosDBQueueMessage> ?? result.ToList();
            if (cosmosDbQueueMessages.Any())
            {
                var docs = (JArray)JsonConvert.DeserializeObject(json);
                for (int i = docs.Count - 1; i >= 0; --i)
                {
                    CosmosDBQueueMessage queueItem = cosmosDbQueueMessages.ElementAt(i);
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

            return cosmosDbQueueMessages;
        }

        internal async Task CompleteAsync(string id)
        {
            Uri documentUri = UriFactory.CreateDocumentUri(
                this.settings.QueueCollectionDefinition.DbName,
                this.settings.QueueCollectionDefinition.CollectionName,
                id);

            await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.DeleteDocumentAsync(documentUri));
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls
        

        /// <inheritdoc />
        public string Name { get; }

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
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.documentClientWithSerializationSettings?.Dispose();
                    this.documentClientWithSerializationSettings = null;
                }

                this.disposedValue = true;
            }
        }
        

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
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

            bool createStoredProcedure = false;
            try
            {
                Uri storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, DequeueItemsStoredProcedureName);
                await this.documentClientWithSerializationSettings.ReadStoredProcedureAsync(storedProcedureUri);

            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    createStoredProcedure = true;
            }

            if (createStoredProcedure)
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName);
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

                ResourceResponse<StoredProcedure> createStoredProcedureResponse = await this.documentClientWithSerializationSettings.CreateStoredProcedureAsync(collectionUri, storedProcedure);
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
            if (this.DeletedCompletedItems)
            {
                await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, cosmosQueueMessage.Id),
                    new RequestOptions
                    {
                        AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                        {
                            Condition = cosmosQueueMessage.ETag,
                            Type = AccessConditionType.IfMatch
                        }
                    }));
            }
            else
            { 
                cosmosQueueMessage.Status = QueueItemStatus.Completed;
                cosmosQueueMessage.TimeToLive = (int)Utils.ToUnixTime(DateTime.UtcNow.AddHours(1));
                await this.SaveQueueItem(cosmosQueueMessage);
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
                    Type = AccessConditionType.IfMatch
                }
            };

            TaskMessage attachmentTaskMessage = null;
            var messageData = message.Data as MessageData;
            if (messageData != null)
            {
                if (this.MaxTaskMessageLength > 0)
                {
                    if (messageData.TaskMessage != null)
                    {
                        string serializedTaskMessage = JsonConvert.SerializeObject(messageData.TaskMessage);
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

            ResourceResponse<Document> createDocumentResponse = await Utils.ExecuteWithRetries(() => this.documentClientWithSerializationSettings.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName), message, reqOptions));
            
            if (attachmentTaskMessage != null)
            {
                Uri createdDocumentUri = UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName, createDocumentResponse.Resource.Id);
                ResourceResponse<Attachment> attachmentResponse = await Utils.SaveAttachment(this.documentClientWithSerializationSettings, createdDocumentUri, attachmentTaskMessage, TaskMessageAttachmentId);

                // set the task message back in the instance
                messageData.TaskMessage = attachmentTaskMessage;
            }

            return createDocumentResponse;
        }


        /// <inheritdoc />
        public async Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields updateFields, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var cosmosDbQueueMessage = (CosmosDBQueueMessage)originalQueueMessage;
            if (updateFields == MessageUpdateFields.Visibility)
            {
                // We "abandon" the message by settings its visibility timeout to zero.
                // This allows it to be reprocessed on this node or another node.
                cosmosDbQueueMessage.Status = QueueItemStatus.Pending;
                cosmosDbQueueMessage.LockedUntil = 0;

                await this.SaveQueueItem(cosmosDbQueueMessage);
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
            int count = this.documentClientWithSerializationSettings
                .CreateDocumentQuery<CosmosDBQueueMessage>(UriFactory.CreateDocumentCollectionUri(settings.QueueCollectionDefinition.DbName, settings.QueueCollectionDefinition.CollectionName))
                .Count(x => x.Status == QueueItemStatus.Pending && x.NextVisibleTime <= Utils.ToUnixTime(DateTime.UtcNow));

            return Task.FromResult<int>(count);
        }

        /// <inheritdoc />
        public async Task<bool> ExistsAsync()
        {            
            try
            {
                Uri storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, DequeueItemsStoredProcedureName);
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
