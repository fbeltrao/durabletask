﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.AzureStorage;
using DurableTask.AzureStorage.Monitoring;
using DurableTask.Core;
using DurableTask.CosmosDB.Monitoring;
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
        ConcurrentDictionary<string, IQueue> allControlQueues;
        ConcurrentDictionary<string, IQueue> ownedControlQueues;
        IQueue workItemQueue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="stats"></param>
        public CosmosDBQueueManager(IExtensibleOrchestrationServiceSettings settings, AzureStorageOrchestrationServiceStats stats)
        {
            this.settings = settings;
            this.stats = stats;
            this.allControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.ownedControlQueues = new ConcurrentDictionary<string, IQueue>();

            var workItemQueueName = $"{this.settings.TaskHubName.ToLowerInvariant()}-workitems";
            this.workItemQueue = GetQueue(workItemQueueName, isControlQueue: false);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = GetQueue(Utils.GetControlQueueId(this.settings.TaskHubName, i), isControlQueue: true);
                this.allControlQueues.TryAdd(queue.Name, queue);
            }
        }

        CosmosDBQueue GetQueue(string name, bool isControlQueue)
        {
            if (isControlQueue)
                return (CosmosDBQueue)this.allControlQueues.GetOrAdd(name, (queueName) => CreateQueue(queueName, isControlQueue));

            return (CosmosDBQueue)
                (this.workItemQueue ?? (this.workItemQueue = CreateQueue(name, isControlQueue)));
        }

        private CosmosDBQueue CreateQueue(string queueName, bool isControlQueue)
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
                },
                UseOneCollectionPerQueueType = true
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
                // NextAvailableTime will be part of "where" and "order by" in queries
                new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessage.NextAvailableTime)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>
                    (
                        new Index[]
                        {
                            new HashIndex(DataType.String, -1),
                            new RangeIndex(DataType.Number, -1),
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
                    string.Concat("/", nameof(CosmosDBQueueMessage.PartitionKey))
                };

                // Add the PartitionKey to the index
                includedIndexes.Add(
                    new IncludedPath()
                    {
                        Path = $"/{nameof(CosmosDBQueueMessage.PartitionKey)}/?",
                        Indexes = new System.Collections.ObjectModel.Collection<Index>
                        (
                            new Index[]
                            {
                                new HashIndex(DataType.String, -1)
                            }
                        )
                    });
            }

            var queue = new CosmosDBQueue(queueSettings, queueName, isControlQueue);
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


        Monitoring.TelemetryRecorder MonitorDependency(string dependencyType) => new Monitoring.TelemetryRecorder(TelemetryClientProvider.TelemetryClient, dependencyType);

        int workItemQueueWriteIndex = -1;
        int workItemQueueReadIndex = -1;

        /// <inheritdoc />
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions)
        {
            using (var recorded = MonitorDependency(nameof(EnqueueMessageAsync)))
            {
                var cosmosQueue = (CosmosDBQueue)queue;

                Guid outboundTraceActivityId = Guid.NewGuid();
                var data = new MessageData(taskMessage, outboundTraceActivityId, queue.Name);
                var msg = new CosmosDBQueueMessage
                {
                    Data = data,
                };


                if (!object.ReferenceEquals(cosmosQueue, this.workItemQueue))
                {
                    msg.PartitionKey = queue.Name;
                }
                else
                {
                    var indexToUse = this.workItemQueueWriteIndex;
                    lock (this)
                    {
                        indexToUse++;
                        if (indexToUse >= this.settings.PartitionCount)
                            indexToUse = 0;

                        this.workItemQueueWriteIndex = indexToUse;
                    }

                    msg.PartitionKey = GetWorkItemQueuePartitionKey(indexToUse);
                }

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await cosmosQueue.Enqueue(msg);

                recorded.AsSucceeded();
            }
        }


        /// <inheritdoc />
        public async Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            using (var recorded = MonitorDependency(nameof(AddMessageAsync)))
            {
                var cosmosQueue = (CosmosDBQueue)queue;

                // We transfer to a new trace activity ID every time a new outbound queue message is created.
                Guid outboundTraceActivityId = Guid.NewGuid();

                var data = new MessageData(message, outboundTraceActivityId, queue.Name);

                var msg = new CosmosDBQueueMessage
                {
                    Data = data,
                };


                if (!object.ReferenceEquals(cosmosQueue, this.workItemQueue))
                {
                    msg.PartitionKey = queue.Name;
                }
                else
                {

                    var indexToUse = this.workItemQueueWriteIndex;
                    lock (this)
                    {
                        indexToUse++;
                        if (indexToUse >= this.settings.PartitionCount)
                            indexToUse = 0;

                        this.workItemQueueWriteIndex = indexToUse;
                    }


                    var partitionKey = GetWorkItemQueuePartitionKey(indexToUse);
                    msg.PartitionKey = partitionKey;
                }

                if (timeToLive.HasValue)
                {
                    msg.TimeToLive = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(timeToLive.Value));
                }

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await cosmosQueue.Enqueue(msg);

                recorded.AsSucceeded();
            }
        }

        string GetWorkItemQueuePartitionKey(int index) => $"{settings.TaskHubName}-workitem-{index}";

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
        public IQueue GetControlQueue(string id)
        {
            return GetQueue(id, isControlQueue: true);
        }

        /// <inheritdoc />
        public Task<IQueue[]> GetControlQueuesAsync(int partitionCount)
        {

            var controlQueues = new IQueue[partitionCount];

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = GetQueue(Utils.GetControlQueueId(this.settings.TaskHubName, i), isControlQueue: true);
                controlQueues[i] = queue;
            }

            return Task.FromResult(controlQueues);
        }


        int batchWorkItemSize = 1;
        System.Collections.Concurrent.ConcurrentQueue<CosmosDBQueueMessage> availableWorkItemMessages = new ConcurrentQueue<CosmosDBQueueMessage>();

        /// <inheritdoc />
        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            using (var recorded = MonitorDependency(nameof(GetMessageAsync)))
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
                        if (object.ReferenceEquals(wq, this.workItemQueue))
                        {
                            for (int i = 0; i < this.settings.PartitionCount; ++i)
                            {
                                this.workItemQueueReadIndex++;
                                if (this.workItemQueueReadIndex >= this.settings.PartitionCount)
                                    this.workItemQueueReadIndex = 0;

                                var partitionKey = GetWorkItemQueuePartitionKey(workItemQueueReadIndex);

                                queueMessage = ((CosmosDBQueueMessage)await wq.GetMessageInPartitionAsync(
                                    partitionKey,
                                    queueVisibilityTimeout,
                                    requestOptions,
                                    operationContext,
                                    cancellationToken));

                                if (queueMessage != null)
                                    break;
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
                    }


                    this.stats.CosmosDBRequests.Increment();
                }

                recorded.AsSucceeded();

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
            using (var recorded = MonitorDependency(nameof(GetMessagesAsync)))
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

                recorded.AsSucceeded();

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