using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.AzureStorage;
using DurableTask.AzureStorage.Monitoring;
using DurableTask.Core;
using Microsoft.Azure.Documents;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Queue manager based on CosmosDB
    /// </summary>
    class CosmosDBQueueManager : IQueueManager
    {
        private readonly IExtensibleOrchestrationServiceSettings settings;
        private readonly AzureStorageOrchestrationServiceStats stats;
        private readonly Microsoft.ApplicationInsights.TelemetryClient telemetryClient;
        ConcurrentDictionary<string, IQueue> allControlQueues;
        ConcurrentDictionary<string, IQueue> ownedControlQueues;
        IQueue workItemQueue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="stats"></param>
        /// <param name="telemetryClient"></param>
        public CosmosDBQueueManager(IExtensibleOrchestrationServiceSettings settings, AzureStorageOrchestrationServiceStats stats, Microsoft.ApplicationInsights.TelemetryClient telemetryClient)
        {
            this.settings = settings;
            this.stats = stats;
            this.telemetryClient = telemetryClient;
            this.allControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.ownedControlQueues = new ConcurrentDictionary<string, IQueue>();

            var workItemQueueName = $"{this.settings.TaskHubName.ToLowerInvariant()}-workitems";
            this.workItemQueue = GetQueue(workItemQueueName);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = GetQueue(Utils.GetControlQueueId(this.settings.TaskHubName, i));
                this.allControlQueues.TryAdd(queue.Name, queue);
            }
        }

        CosmosDBQueue GetQueue(string name)
        {
            var queueSettings = new CosmosDBQueueSettings
            {
                QueueCollectionDefinition = new CosmosDBCollectionDefinition
                {
                    CollectionName = "queue",
                    DbName = this.settings.CosmosDBName,
                    Endpoint = this.settings.CosmosDBEndpoint,
                    SecretKey = this.settings.CosmosDBAuthKey,
                    Throughput = this.settings.CosmosDBQueueCollectionThroughput,
                }
            };

            // Exclude all
            queueSettings.QueueCollectionDefinition.IndexExcludedPaths = new ExcludedPath[]
            {
                new ExcludedPath()
                {
                    Path = "/*"
                }
            };

            var includedIndexes = new List<IncludedPath>()
            {
                // NextVisibleTime will be part of "where" and "order by" in queries
                new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessage.NextVisibleTime)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>(
                        new Index[]
                        {
                            new HashIndex(DataType.String, -1),
                            new RangeIndex(DataType.Number, -1),
                        }
                    )
                },

                // Status will be part of "where" in queries
                new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessage.Status)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>(
                        new Index[]
                        {
                            new HashIndex(DataType.String, -1)
                        }
                    )
                },

                // LockedUntil will be part of "where" in queries
                new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessage.LockedUntil)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>(
                        new Index[]
                        {
                            new HashIndex(DataType.Number, -1)
                        }
                    )
                }

            };
            queueSettings.QueueCollectionDefinition.IndexIncludedPaths = includedIndexes;

            if (!this.settings.CosmosDBQueueUsePartition)
            {
                // QueueName will be part of "where" in queries
                includedIndexes.Add(new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessage.QueueName)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>(
                        new Index[]
                        {
                            new HashIndex(DataType.String, -1)
                        }
                    )
                });
            }


            if (this.settings.CosmosDBQueueUsePartition)
            {
                // Set the queue name as the partition key
                queueSettings.QueueCollectionDefinition.PartitionKeyPaths = new System.Collections.ObjectModel.Collection<string>
                {
                    string.Concat("/", nameof(CosmosDBQueueMessage.QueueName))
                };
            }



            var queue = new CosmosDBQueue(queueSettings, name);
            return queue;
        }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> AllControlQueues => allControlQueues;

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> OwnedControlQueues => ownedControlQueues;

        /// <inheritdoc />
        public IQueue WorkItemQueue => workItemQueue;

        /// <inheritdoc />
        public string StorageName => this.settings.CosmosDBEndpoint;


        IDisposable MonitorDependency(string dependencyType) => new Monitoring.TelemetryRecorder(this.telemetryClient, dependencyType);


        /// <inheritdoc />
        public async Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            using (MonitorDependency(nameof(AddMessageAsync)))
            {
                var cosmosQueue = (CosmosDBQueue)queue;

                // We transfer to a new trace activity ID every time a new outbound queue message is created.
                Guid outboundTraceActivityId = Guid.NewGuid();

                var data = new MessageData(message, outboundTraceActivityId, queue.Name);

                var msg = new CosmosDBQueueMessage
                {
                    Data = data,
                };

                if (timeToLive.HasValue)
                {
                    msg.TimeToLive = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(timeToLive.Value));
                }

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await cosmosQueue.Enqueue(msg);
            }
        }

        /// <inheritdoc />
        public async Task DeleteAsync()
        {
            foreach (string partitionId in this.allControlQueues.Keys)
            {
                if (this.allControlQueues.TryGetValue(partitionId, out var controlQueue))
                {
                    await controlQueue.DeleteIfExistsAsync();
                }
            }

            await this.workItemQueue.DeleteIfExistsAsync();
        }

        /// <inheritdoc />
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions)
        {
            using (MonitorDependency(nameof(EnqueueMessageAsync)))
            {
                var cosmosQueue = (CosmosDBQueue)queue;

                Guid outboundTraceActivityId = Guid.NewGuid();
                var data = new MessageData(taskMessage, outboundTraceActivityId, queue.Name);
                var msg = new CosmosDBQueueMessage
                {
                    Data = data,
                };

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await cosmosQueue.Enqueue(msg);
            }
        }

        /// <inheritdoc />
        public IQueue GetControlQueue(string id)
        {
            return GetQueue(id);
        }

        /// <inheritdoc />
        public Task<IQueue[]> GetControlQueuesAsync(int partitionCount)
        {

            var controlQueues = new IQueue[partitionCount];

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = GetQueue(Utils.GetControlQueueId(this.settings.TaskHubName, i));
                controlQueues[i] = queue;
            }

            return Task.FromResult(controlQueues);
        }


        int batchWorkItemSize = 10;
        System.Collections.Concurrent.ConcurrentQueue<CosmosDBQueueMessage> availableWorkItemMessages = new ConcurrentQueue<CosmosDBQueueMessage>();

        /// <inheritdoc />
        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            using (MonitorDependency(nameof(GetMessageAsync)))
            {
                CosmosDBQueueMessage queueMessage = null;
                if (batchWorkItemSize > 1)
                {
                    availableWorkItemMessages.TryDequeue(out queueMessage);
                }

                if (queueMessage == null)
                {

                    var wq = (CosmosDBQueue)queue;
                    if (batchWorkItemSize > 1)
                    {
                        var queueMessages = ((IEnumerable<CosmosDBQueueMessage>)await wq.GetMessagesAsync(
                            batchWorkItemSize,
                            queueVisibilityTimeout,
                            requestOptions,
                            operationContext,
                            cancellationToken));

                        if (queueMessages != null)
                        {
                            foreach (var qm in queueMessages)
                            {
                                availableWorkItemMessages.Enqueue(qm);
                            }

                            availableWorkItemMessages.TryDequeue(out queueMessage);
                        }
                    }
                    else
                    {
                        queueMessage = ((CosmosDBQueueMessage)await wq.GetMessageAsync(
                            queueVisibilityTimeout,
                            requestOptions,
                            operationContext,
                            cancellationToken));
                    }
                    

                    this.stats.CosmosDBRequests.Increment();
                }

                if (queueMessage != null)
                {
                    if (queueMessage.Data is MessageData messageData)
                    {
                        return ReceivedMessageContext.CreateFromReceivedMessage(messageData, this.StorageName, settings.TaskHubName, queueMessage.Id, queue.Name);
                    }
                }

                return null;
            }
        }

        /// <inheritdoc />
        public async Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
        {
            using (MonitorDependency(nameof(GetMessagesAsync)))
            {
                var messages = new List<MessageData>();

                await this.ownedControlQueues.Values.ParallelForEachAsync(
                    async delegate (IQueue controlQueue)
                    {
                        var batch = await controlQueue.GetMessagesAsync(
                            this.settings.ControlQueueBatchSize,
                            this.settings.ControlQueueVisibilityTimeout,
                            ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                            null /* operationContext */,
                            cancellationToken);

                        this.stats.CosmosDBRequests.Increment();

                        if (batch != null)
                        {
                            var incomingMessages = batch.Select(x => (MessageData)((CosmosDBQueueMessage)x).Data);

                            lock (messages)
                            {
                                messages.AddRange(incomingMessages);
                            }
                        }
                    });

                return messages;
            }
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            // for now the queues are in a single collection
            await this.workItemQueue.CreateIfNotExistsAsync();

            //foreach (var queue in this.allControlQueues.Values)
            //    await queue.CreateIfNotExistsAsync();
        }
    }
}
