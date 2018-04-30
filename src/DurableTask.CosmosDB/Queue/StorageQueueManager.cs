using DurableTask.AzureStorage;
using DurableTask.AzureStorage.Monitoring;
using DurableTask.AzureStorage.Partitioning;
using DurableTask.Core;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Storage based implementation of queue manager
    /// </summary>
    internal class StorageQueueManager : IQueueManager
    {
        private readonly IExtensibleOrchestrationServiceSettings settings;
        private readonly AzureStorageOrchestrationServiceStats stats;
        readonly CloudQueueClient queueClient;

        readonly ConcurrentDictionary<string, IQueue> ownedControlQueues;
        readonly ConcurrentDictionary<string, IQueue> allControlQueues;
        readonly IQueue workItemQueue;
        private readonly string storageAccountName;
        readonly CloudBlobClient blobClient;
        internal readonly MessageManager messageManager;


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="stats"></param>
        internal StorageQueueManager(IExtensibleOrchestrationServiceSettings settings, AzureStorageOrchestrationServiceStats stats)
        {
            this.settings = settings;
            this.stats = stats;
            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);
            this.storageAccountName = account.Credentials.AccountName;
            this.queueClient = account.CreateCloudQueueClient();
            this.queueClient.BufferManager = SimpleBufferManager.Shared;
            this.ownedControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.allControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.BufferManager = SimpleBufferManager.Shared;

            string compressedMessageBlobContainerName = $"{settings.TaskHubName.ToLowerInvariant()}-largemessages";
            NameValidator.ValidateContainerName(compressedMessageBlobContainerName);
            this.messageManager = new MessageManager(this.blobClient, compressedMessageBlobContainerName);


            this.workItemQueue = GetWorkItemQueue(account, settings.TaskHubName);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = GetControlQueue(this.queueClient, this.settings.TaskHubName, i);
                this.allControlQueues.TryAdd(queue.Name, queue);
            }
        }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> AllControlQueues { get { return this.allControlQueues; } }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> OwnedControlQueues { get { return this.ownedControlQueues; } }

        /// <inheritdoc />
        public IQueue WorkItemQueue => this.workItemQueue;

        /// <inheritdoc />
        public string StorageName => this.storageAccountName;

        public bool TryGetQueue(string partitionId, out IQueue queue)
        {
            queue = null;
            if (this.allControlQueues.TryGetValue(partitionId, out var wq))
            {
                queue = wq;
                return true;
            }

            return false;
        }

        internal static IQueue GetControlQueue(CloudStorageAccount account, string taskHub, int partitionIndex)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            return GetControlQueue(account.CreateCloudQueueClient(), taskHub, partitionIndex);
        }


        internal static IQueue GetControlQueue(CloudQueueClient queueClient, string taskHub, int partitionIndex)
        {
            return GetQueueInternal(queueClient, taskHub, $"control-{partitionIndex:00}");
        }

        internal static IQueue GetWorkItemQueue(CloudStorageAccount account, string taskHub)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            return GetQueueInternal(account.CreateCloudQueueClient(), taskHub, "workitems");
        }

        static IQueue GetQueueInternal(CloudQueueClient queueClient, string taskHub, string suffix)
        {
            if (queueClient == null)
            {
                throw new ArgumentNullException(nameof(queueClient));
            }

            if (string.IsNullOrEmpty(taskHub))
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            string queueName = $"{taskHub.ToLowerInvariant()}-{suffix}";
            NameValidator.ValidateQueueName(queueName);

            return new CloudQueueWrapper(queueClient.GetQueueReference(queueName));
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
        public Task StartAsync()
        {
            var workItemCloudQueue = ((CloudQueueWrapper)this.workItemQueue).cloudQueue;
            ServicePointManager.FindServicePoint(workItemCloudQueue.Uri).UseNagleAlgorithm = false;
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public IQueue GetQueue(string partitionId)
        {
            this.stats.StorageRequests.Increment();
            return new CloudQueueWrapper(this.queueClient.GetQueueReference(partitionId));
        }

        public async Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
        {
            var messages = new List<MessageData>();

            await this.ownedControlQueues.Values.ParallelForEachAsync(
                async delegate (IQueue controlQueue)
                {
                    IEnumerable<CloudQueueMessage> batch = await controlQueue.GetMessagesAsync(
                        this.settings.ControlQueueBatchSize,
                        this.settings.ControlQueueVisibilityTimeout,
                        ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                        null /* operationContext */,
                        cancellationToken);
                    this.stats.StorageRequests.Increment();

                    IEnumerable<MessageData> deserializedBatch = await Task.WhenAll(
                        batch.Select(async m => await this.messageManager.DeserializeQueueMessageAsync(m, controlQueue.Name)));
                    lock (messages)
                    {
                        messages.AddRange(deserializedBatch);
                    }
                });

            return messages;
        }

        public IQueue GetControlQueue(int partitionIndex)
        {
            return GetControlQueue(this.queueClient, this.settings.TaskHubName, (int)partitionIndex);
        }

        public Task<IQueue> GetControlQueueAsync(string partitionId)
        {
            return null;
            //return GetControlQueueAsync
        }

        internal static async Task<IQueue[]> GetControlQueuesAsync(
            CloudStorageAccount account,
            string taskHub,
            int defaultPartitionCount)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            if (taskHub == null)
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            var inactiveLeaseManager = BlobLeaseManager.GetBlobLeaseManager(taskHub, "n/a", account, TimeSpan.Zero, TimeSpan.Zero, null);
            TaskHubInfo hubInfo = await inactiveLeaseManager.GetOrCreateTaskHubInfoAsync(
                TaskHubInfo.GetTaskHubInfo(taskHub, defaultPartitionCount));

            CloudQueueClient queueClient = account.CreateCloudQueueClient();

            var controlQueues = new IQueue[hubInfo.PartitionCount];
            for (int i = 0; i < hubInfo.PartitionCount; i++)
            {
                controlQueues[i] = GetControlQueue(queueClient, taskHub, i);
            }

            return controlQueues;
        }


        /// <inheritdoc />
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions queueRequestOptions)
        {
            CloudQueueMessage message = await context.CreateOutboundQueueMessageAsync(this.messageManager, taskMessage, queue.Name);
            var wq = (CloudQueueWrapper)queue;
            await wq.cloudQueue.AddMessageAsync(
                message,
                null /* timeToLive */,
                initialVisibilityDelay,
                queueRequestOptions,
                context.StorageOperationContext);
        }

        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var wq = (CloudQueueWrapper)queue;
            var queueMessage = await wq.cloudQueue.GetMessageAsync(
                    queueVisibilityTimeout,
                    requestOptions,
                    operationContext,
                    cancellationToken);

            this.stats.StorageRequests.Increment();

            if (queueMessage != null)
            {
                ReceivedMessageContext context = await ReceivedMessageContext.CreateFromReceivedMessageAsync(
                   this.messageManager,
                   this.storageAccountName,
                   this.settings.TaskHubName,
                   queueMessage,
                   queue.Name);

                return context;
            }

            return null;
        }

        public async Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            var wq = (CloudQueueWrapper)queue;
            var context = await ReceivedMessageContext.CreateOutboundQueueMessageInternalAsync(
                    this.messageManager,
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    queue.Name,
                    message);

            await wq.cloudQueue.AddMessageAsync(
                context,
                timeToLive,
                initialVisibilityDelay,
                requestOptions,
                operationContext);

            this.stats.StorageRequests.Increment();
        }

        /// <inheritdoc />
        public async Task<IQueue[]> GetControlQueuesAsync(int partitionCount)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);

            var inactiveLeaseManager = BlobLeaseManager.GetBlobLeaseManager(this.settings.TaskHubName, "n/a", account, TimeSpan.Zero, TimeSpan.Zero, null);
            var hubInfo = await inactiveLeaseManager.GetOrCreateTaskHubInfoAsync(
                TaskHubInfo.GetTaskHubInfo(this.settings.TaskHubName, partitionCount));

            CloudQueueClient queueClient = account.CreateCloudQueueClient();

            var controlQueues = new IQueue[hubInfo.PartitionCount];
            for (int i = 0; i < hubInfo.PartitionCount; i++)
            {
                controlQueues[i] = GetControlQueue(queueClient, this.settings.TaskHubName, i);
            }

            return controlQueues;
        }
    }
}
