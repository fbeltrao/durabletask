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
                    CollectionName = "queue2",
                    DbName = this.settings.CosmosDBName,
                    Endpoint = this.settings.CosmosDBEndpoint,
                    SecretKey = this.settings.CosmosDBAuthKey,
                }
            };

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

        /// <inheritdoc />
        public async Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
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

        /// <inheritdoc />
        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var wq = (CosmosDBQueue)queue;
            var queueMessage = ((CosmosDBQueueMessage)await wq.GetMessageAsync(
                    queueVisibilityTimeout,
                    requestOptions,
                    operationContext,
                    cancellationToken));

            this.stats.CosmosDBRequests.Increment();

            if (queueMessage != null)
            {
                if (queueMessage.Data is MessageData messageData)
                {
                    return ReceivedMessageContext.CreateFromReceivedMessage(messageData, this.StorageName, settings.TaskHubName, queueMessage.Id, queue.Name);
                }
            }

            return null;
        }

        /// <inheritdoc />
        public async Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
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

        /// <inheritdoc />
        public async Task StartAsync()
        {
            await this.workItemQueue.CreateIfNotExistsAsync();

            foreach (var queue in this.allControlQueues.Values)
                await queue.CreateIfNotExistsAsync();
        }
    }
}
