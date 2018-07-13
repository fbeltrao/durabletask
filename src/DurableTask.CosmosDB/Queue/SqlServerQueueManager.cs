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
    /// Queue manager based on SQL Server
    /// </summary>
    class SqlServerQueueManager : IQueueManager
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
        public SqlServerQueueManager(IExtensibleOrchestrationServiceSettings settings, AzureStorageOrchestrationServiceStats stats)
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

        SqlServerQueue GetQueue(string name, bool isControlQueue)
        {
            if (isControlQueue)            
                return (SqlServerQueue)this.allControlQueues.GetOrAdd(name, (queueName) => CreateQueue(queueName, isControlQueue));

            return (SqlServerQueue)
                (this.workItemQueue ?? (this.workItemQueue = CreateQueue(name, isControlQueue)));            
        }

        SqlServerQueue CreateQueue(string name, bool isControlQueue)
        {
            var queueSettings = new SqlServerQueueSettings
            {
                ConnectionString = settings.SqlConnectionString,
                UseMemoryOptimizedTable = settings.SqlQueueUseMemoryOptimizedTable
            };

            var queue = new SqlServerQueue(queueSettings, name, isControlQueue);
            return queue;
        }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> AllControlQueues => allControlQueues;

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> OwnedControlQueues => ownedControlQueues;

        /// <inheritdoc />
        public IQueue WorkItemQueue => workItemQueue;

        /// <inheritdoc />
        public string StorageName => "SQL";


        Monitoring.TelemetryRecorder MonitorDependency(string dependencyType) => new Monitoring.TelemetryRecorder(TelemetryClientProvider.TelemetryClient, dependencyType);


        /// <inheritdoc />
        public async Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            using (var recorded = MonitorDependency(nameof(AddMessageAsync)))
            {
                var targetQueue = (SqlServerQueue)queue;

                // We transfer to a new trace activity ID every time a new outbound queue message is created.
                Guid outboundTraceActivityId = Guid.NewGuid();

                var data = new MessageData(message, outboundTraceActivityId, queue.Name);

                var msg = new SqlServerQueueMessage(data);

                if (timeToLive.HasValue)
                {
                    msg.TimeToLive = (int)Utils.ToUnixTime(DateTime.UtcNow.Add(timeToLive.Value));
                }

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await targetQueue.Enqueue(msg);

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
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions)
        {
            using (var recorded = MonitorDependency(nameof(EnqueueMessageAsync)))
            {
                var targetQueue = (SqlServerQueue)queue;

                Guid outboundTraceActivityId = Guid.NewGuid();
                var data = new MessageData(taskMessage, outboundTraceActivityId, queue.Name);
                var msg = new SqlServerQueueMessage(data);

                if (initialVisibilityDelay.HasValue)
                {
                    msg.NextVisibleTime = Utils.ToUnixTime(DateTime.UtcNow.Add(initialVisibilityDelay.Value));
                }

                await targetQueue.Enqueue(msg);

                recorded.AsSucceeded();
            }
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
        System.Collections.Concurrent.ConcurrentQueue<SqlServerQueueMessage> availableWorkItemMessages = new ConcurrentQueue<SqlServerQueueMessage>();

        /// <inheritdoc />
        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            using (var recorded = MonitorDependency(nameof(GetMessageAsync)))
            {
                SqlServerQueueMessage queueMessage = null;
                if (batchWorkItemSize > 1)
                {
                    availableWorkItemMessages.TryDequeue(out queueMessage);
                }

                if (queueMessage == null)
                {

                    var wq = (SqlServerQueue)queue;
                    if (batchWorkItemSize > 1)
                    {
                        var queueMessages = ((IEnumerable<SqlServerQueueMessage>)await wq.GetMessagesAsync(
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
                        queueMessage = ((SqlServerQueueMessage)await wq.GetMessageAsync(
                            queueVisibilityTimeout,
                            requestOptions,
                            operationContext,
                            cancellationToken));
                    }
                    

                    this.stats.CosmosDBRequests.Increment();
                }

                recorded.AsSucceeded();

                if (queueMessage != null)
                {
                    if (queueMessage.GetObjectData() is MessageData messageData)
                    {
                        return ReceivedMessageContext.CreateFromReceivedMessage(messageData, this.StorageName, settings.TaskHubName, queueMessage.QueueItemID.ToString(), queue.Name);
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
                            var incomingMessages = batch.Select(x => (MessageData)((SqlServerQueueMessage)x).GetObjectData());

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
