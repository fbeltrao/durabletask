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
        private CosmosDBQueueWrapper controlQueue;

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

            // A single queue can handle the control queue (partition key)
            this.controlQueue = GetQueue($"{this.settings.TaskHubName.ToLowerInvariant()}-control", isControlQueue: true);
            this.allControlQueues.TryAdd(controlQueue.Name, controlQueue);
            this.ownedControlQueues.TryAdd(controlQueue.Name, controlQueue);
        }

        CosmosDBQueueWrapper GetQueue(string name, bool isControlQueue)
        {
            if (isControlQueue)
            {
                return (CosmosDBQueueWrapper)
                    (this.controlQueue ?? (this.controlQueue = CreateQueue(name, isControlQueue)));
            }

            return (CosmosDBQueueWrapper)
                (this.workItemQueue ?? (this.workItemQueue = CreateQueue(name, isControlQueue)));
        }

        private CosmosDBQueueWrapper CreateQueue(string queueName, bool isControlQueue)
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
                UseOneCollectionPerQueueType = true,
                WorkerId = this.settings.WorkerId,
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
                // PartitionKey
                new IncludedPath()
                {
                    Path = $"/{nameof(CosmosDBQueueMessageWrapper.PartitionKey)}/?",
                    Indexes = new System.Collections.ObjectModel.Collection<Index>(
                        new Index[]
                        {
                            new HashIndex(DataType.String, -1)
                        }
                    )
                }
            };

            queueSettings.QueueCollectionDefinition.IndexIncludedPaths = includedIndexes;

            // Set the queue name as the partition key
            queueSettings.QueueCollectionDefinition.PartitionKeyPaths = new System.Collections.ObjectModel.Collection<string>
            {
                string.Concat("/", nameof(CosmosDBQueueMessageWrapper.PartitionKey))
            };

            var queue = new CosmosDBQueueWrapper(queueSettings, queueName, isControlQueue);
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
        
        /// <inheritdoc />
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions)
        {
            using (var recorded = MonitorDependency(nameof(EnqueueMessageAsync)))
            {
                var cosmosQueue = (CosmosDBQueueWrapper)queue;

                Guid outboundTraceActivityId = Guid.NewGuid();
                var data = new MessageData(taskMessage, outboundTraceActivityId, queue.Name);
                var msg = new CosmosDBQueueMessageWrapper().SetDataContent(data);

                if (cosmosQueue.isControlQueue)
                    msg.PartitionKey = taskMessage.OrchestrationInstance.InstanceId;
                else
                    msg.PartitionKey = Guid.NewGuid().ToString();

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
                var cosmosQueue = (CosmosDBQueueWrapper)queue;

                // We transfer to a new trace activity ID every time a new outbound queue message is created.
                Guid outboundTraceActivityId = Guid.NewGuid();

                var data = new MessageData(message, outboundTraceActivityId, queue.Name);

                var msg = new CosmosDBQueueMessageWrapper().SetDataContent(data);

                if (cosmosQueue.isControlQueue)
                    msg.PartitionKey = message.OrchestrationInstance.InstanceId;
                else
                    msg.PartitionKey = Guid.NewGuid().ToString();

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
            return Task.FromResult(new IQueue[] { this.controlQueue });
        }


        System.Collections.Concurrent.ConcurrentQueue<CosmosDBQueueMessageWrapper> availableWorkItemMessages = new ConcurrentQueue<CosmosDBQueueMessageWrapper>();

        /// <inheritdoc />
        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            using (var recorded = MonitorDependency(nameof(GetMessageAsync)))
            {
                availableWorkItemMessages.TryDequeue(out var queueMessage);                

                if (queueMessage == null)
                {

                    var wq = (CosmosDBQueueWrapper)queue;
                    var queueMessages = ((IEnumerable<CosmosDBQueueMessageWrapper>)await wq.GetMessagesAsync(
                        1,
                        queueVisibilityTimeout,
                        requestOptions,
                        operationContext,
                        cancellationToken));

                    if (queueMessages != null)
                    {
                        queueMessage = queueMessages.FirstOrDefault();
                        if (queueMessages.Count() > 1)
                        {
                            foreach (var qm in queueMessages.Skip(1))
                            {
                                availableWorkItemMessages.Enqueue(qm);
                            }                           
                        }
                    }                    

                    this.stats.CosmosDBRequests.Increment();
                }

                recorded.AsSucceeded();

                if (queueMessage != null)
                {
                    if (queueMessage.Contents.Data is MessageData messageData)
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
                var rawMessages = await this.controlQueue.GetMessagesAsync(
                    this.settings.ControlQueueBatchSize,
                            this.settings.ControlQueueVisibilityTimeout,
                            ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                            null /* operationContext */,
                            cancellationToken);

                this.stats.CosmosDBRequests.Increment();
                var messages = rawMessages.Select(x => (MessageData)(((CosmosDBQueueMessageWrapper)x).GetDataContent())).ToList();

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
