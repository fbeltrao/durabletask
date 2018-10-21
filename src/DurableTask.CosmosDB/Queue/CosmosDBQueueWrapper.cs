using DocumentDB.Queue;
using DurableTask.AzureStorage;
using DurableTask.Core;
using DurableTask.CosmosDB.Monitoring;
using DurableTask.CosmosDB.Queue;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Reader;
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
    public class CosmosDBQueueWrapper : IDisposable, IQueue
    {
        private readonly bool queueIsPartitioned;
        private CosmosDBQueueSettings settings;

        // Implement a better static usage, where we can have multiple cosmosdb accounts
        CosmosDBQueue internalQueue;
        bool initialized = false;
        private readonly string queueName;
        internal readonly bool isControlQueue;
        private readonly string collectionName;
        private readonly Uri queueCollectionUri;
        private static JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Objects
        };

        //const int MaxTaskMessageLength = 200000;

        /// <summary>
        /// Indicate if completed queue items should be deleted
        /// </summary>
        public bool DeletedCompletedItems { get; set; } = true;

        /// <summary>
        /// Biggest size of message to be handled inline
        /// </summary>
        public int MaxTaskMessageLength { get; set; } = 0;


        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueueWrapper(CosmosDBQueueSettings settings, string name, bool isControlQueue)
        {
            //this.queueIsPartitioned = !isControlQueue && settings.QueueCollectionDefinition.PartitionKeyPaths?.Count > 0;
            this.queueIsPartitioned = settings.QueueCollectionDefinition.PartitionKeyPaths?.Count > 0;
            this.settings = settings;
            this.queueName = name;
            this.isControlQueue = isControlQueue;
            this.collectionName = this.settings.QueueCollectionDefinition.CollectionName;            
            if (settings.UseOneCollectionPerQueueType)
            {
                this.collectionName = $"{this.settings.QueueCollectionDefinition.CollectionName}-{(isControlQueue ? "control" : "workitem")}";
            }

            this.queueCollectionUri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.collectionName);
        }

        /// <summary>
        /// Enqueues a message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        internal async Task<CosmosDBQueueMessageWrapper> Enqueue(CosmosDBQueueMessageWrapper message)
        {
            if (!this.initialized)
                throw new InvalidOperationException("Must call Initialize() first");

            message.InsertionTime = DateTime.UtcNow;
            if (message.NextVisibleTime.HasValue)
                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow);
            
            var newDoc = await SaveQueueItem(message);
            message.UpdateData(newDoc);

            return message;
        }

        async Task<IEnumerable<IQueueMessage>> DequeueAsync(int batchSize, TimeSpan visibilityTimeout, string partitionKey, CancellationToken cancellationToken)
        {
            var items = await internalQueue.Dequeue();
                       
            System.Diagnostics.Trace.WriteLine($"Queue: {this.queueName} dequeued {items.Count} items");

            return await BuildQueueItems(items);            
        }


        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            controlQueueBatchSize = Math.Max(controlQueueBatchSize, 20);
            return await DequeueAsync(controlQueueBatchSize, controlQueueVisibilityTimeout, this.queueName, cancellationToken);
        }


        internal async Task<IQueueMessage> GetMessageAsync(TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return (await DequeueAsync(1, queueVisibilityTimeout, this.queueName, cancellationToken)).FirstOrDefault();
        }

        internal async Task<IQueueMessage> GetMessageInPartitionAsync(string partitionKey, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return (await DequeueAsync(1, queueVisibilityTimeout, partitionKey, cancellationToken)).FirstOrDefault();
        }


        private Task<IEnumerable<IQueueMessage>> BuildQueueItems(IReadOnlyList<CosmosDBQueueMessage> rawMessages)
        {
            var result = new List<CosmosDBQueueMessageWrapper>();

            foreach (var rawMessage in rawMessages)
            {
                var messageContent = JsonConvert.DeserializeObject<MessageContent>(rawMessage.Data.ToString(), jsonSerializerSettings);
                var messageWrapper = new CosmosDBQueueMessageWrapper(rawMessage, messageContent);
                var messageData = (MessageData)messageWrapper.GetDataContent();
                messageData.OriginalQueueMessage = messageWrapper;
                messageData.QueueName = messageWrapper.PartitionKey;
                messageData.TotalMessageSizeBytes = 0;

                result.Add(messageWrapper);
            }            

            return Task.FromResult<IEnumerable<IQueueMessage>>(result);
        }

        internal async Task CompleteAsync(string id)
        {
            var documentUri = UriFactory.CreateDocumentUri(
                settings.QueueCollectionDefinition.DbName,
                this.collectionName,
                id);

            try
            {
                await this.internalQueue.Complete(id);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }




        /// <inheritdoc />
        public string Name => this.queueName;

        static DocumentClient documentClient;

        static SemaphoreSlim creationLock = new SemaphoreSlim(1);

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            if (this.initialized)
                return;


            await creationLock.WaitAsync();

            try
            {
                var endpointUri = new Uri(this.settings.QueueCollectionDefinition.Endpoint);
                var isLocalCosmosDB = endpointUri.DnsSafeHost.Equals("localhost", StringComparison.InvariantCultureIgnoreCase);
                
                if (documentClient == null)
                {
                    ConnectionPolicy connectionPolicy = null;
                    if (!isLocalCosmosDB)
                    {
                        connectionPolicy = new ConnectionPolicy()
                        {
                            ConnectionMode = ConnectionMode.Direct,
                            ConnectionProtocol = Protocol.Tcp
                        };
                    }

                    documentClient = new DocumentClient(
                        endpointUri, 
                        this.settings.QueueCollectionDefinition.SecretKey,
                        jsonSerializerSettings,
                        connectionPolicy);
                }

            

                await TryExecute(async () => await documentClient.CreateDatabaseIfNotExistsAsync(new Database() { Id = this.settings.QueueCollectionDefinition.DbName }));
                await TryExecute(async () =>
                    await documentClient.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(this.collectionName),
                        new DocumentCollection
                        {
                            Id = this.collectionName,
                            IndexingPolicy = new IndexingPolicy
                            {
                                ExcludedPaths = new System.Collections.ObjectModel.Collection<ExcludedPath>(this.settings.QueueCollectionDefinition.IndexExcludedPaths),
                                IncludedPaths = new System.Collections.ObjectModel.Collection<IncludedPath>(this.settings.QueueCollectionDefinition.IndexIncludedPaths),
                            },
                            PartitionKey = new PartitionKeyDefinition
                            {
                                Paths = this.settings.QueueCollectionDefinition.PartitionKeyPaths
                            },
                        },
                        isLocalCosmosDB ? null : new RequestOptions { OfferThroughput = this.settings.QueueCollectionDefinition.Throughput })
                        );


                var leaseCollectionName = $"{this.collectionName}-lease";

                await TryExecute(async () => await documentClient.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(this.settings.QueueCollectionDefinition.DbName), new DocumentCollection() { Id = leaseCollectionName }));

                var builder = new ChangeFeedReaderBuilder()
                    .WithHostName($"{this.settings.WorkerId}-{this.collectionName}")
                    .WithFeedDocumentClient(documentClient)                    
                    .WithFeedCollection(new DocumentCollectionInfo()
                    {
                        Uri = new Uri(this.settings.QueueCollectionDefinition.Endpoint),
                        MasterKey = this.settings.QueueCollectionDefinition.SecretKey,
                        CollectionName = this.collectionName,
                        DatabaseName = this.settings.QueueCollectionDefinition.DbName,
                    })
                    .WithProcessorOptions(new ChangeFeedProcessorOptions
                    {
                        MaxItemCount = this.isControlQueue ? 20 : 1,
                        StartFromBeginning = true,
                    })
                    .WithLeaseCollection(new DocumentCollectionInfo()
                    {
                        Uri = new Uri(this.settings.QueueCollectionDefinition.Endpoint),
                        MasterKey = this.settings.QueueCollectionDefinition.SecretKey,
                        CollectionName = leaseCollectionName,
                        DatabaseName = this.settings.QueueCollectionDefinition.DbName,
                    });

                var reader = await builder.BuildAsync();

                await reader.StartAsync();

                this.internalQueue = new CosmosDBQueue(reader, builder.GetFeedDocumentClient(), builder.GetFeedCollectionInfo());
                this.initialized = true;
            }
            finally
            {
                creationLock.Release();
            }
        }

        private async Task TryExecute(Func<Task> impl)
        {
            try
            {
                await impl();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
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
            // complete
            var cosmosQueueMessage = (CosmosDBQueueMessageWrapper)queueMessage;
            await this.internalQueue.Complete(cosmosQueueMessage.GetInternalMessage());            
        }

        /// <summary>
        /// Helper to save the queue item
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        async Task<Document> SaveQueueItem(CosmosDBQueueMessageWrapper message)
        {
            var reqOptions = new RequestOptions();

            // updating an existing item, use optimistic locking by _etag
            if (!string.IsNullOrEmpty(message.Id))
            {
                reqOptions.AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition()
                {
                    Condition = message.ETag,
                    Type = AccessConditionType.IfMatch,
                };

            }
            reqOptions.PartitionKey = new PartitionKey(message.PartitionKey);

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

                            messageData.CompressedBlobName = UriFactory.CreateAttachmentUri(settings.QueueCollectionDefinition.DbName, this.collectionName, message.Id, TaskMessageAttachmentId).ToString();
                            if (message.NextVisibleTime == 0)
                                message.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.AddSeconds(5));
                            else
                                message.NextVisibleTime += 5;
                        }
                    }
                }
            }
#endif            
            var contents = message.Contents;
            var createdDocument = await this.internalQueue.Enqueue(contents);

#if INCLUDED_EXPERIMENTAL_ATTACHMENTS
            if (attachmentTaskMessage != null)
            {
                var createdDocumentUri = UriFactory.CreateDocumentUri(settings.QueueCollectionDefinition.DbName, this.collectionName, createDocumentResponse.Resource.Id);
                var attachmentResponse = await Utils.SaveAttachment(this.documentClientWithSerializationSettings, createdDocumentUri, attachmentTaskMessage, TaskMessageAttachmentId);

                // set the task message back in the instance
                messageData.TaskMessage = attachmentTaskMessage;
            }
#endif

            return createdDocument;
        }


        /// <inheritdoc />
        public async Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields updateFields, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var cosmosDBQueueMessage = (CosmosDBQueueMessageWrapper)originalQueueMessage;
            if (updateFields == MessageUpdateFields.Visibility)
            {
                // We "abandon" the message by settings its visibility timeout to zero.
                // This allows it to be reprocessed on this node or another node.
                cosmosDBQueueMessage.NextAvailableTime = cosmosDBQueueMessage.NextVisibleTime;

                await SaveQueueItem(cosmosDBQueueMessage);
            }
        }

        /// <inheritdoc />
        public Task<IQueueMessage> PeekMessageAsync()
        {
            // TODO: implement it
            var qm = new CosmosDBQueueMessageWrapper();
            return Task.FromResult<IQueueMessage>(qm);
            //throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<int> GetQueueLenghtAsync()
        {
            // TODO: implement it
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync() => Task.FromResult(true);

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

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
                //    documentClient?.Dispose();
                //    documentClient = null;
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

        #endregion
    }
}