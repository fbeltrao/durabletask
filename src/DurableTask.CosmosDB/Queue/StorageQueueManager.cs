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
using System.Text;
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


            this.workItemQueue = GetWorkItemQueue(account);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                var queue = this.queueClient.GetQueueReference(Utils.GetControlQueueId(this.settings.TaskHubName, i));                
                this.allControlQueues.TryAdd(queue.Name, new CloudQueueWrapper(queue));
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


   

        IQueue GetWorkItemQueue(CloudStorageAccount account)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            return GetQueueInternal(account.CreateCloudQueueClient(), "workitems");
        }

        IQueue GetQueueInternal(CloudQueueClient queueClient, string suffix)
        {
            if (queueClient == null)
            {
                throw new ArgumentNullException(nameof(queueClient));
            }

            string queueName = $"{this.settings.TaskHubName.ToLowerInvariant()}-{suffix}";
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
        public IQueue GetControlQueue(string partitionId)
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
                    var batch = await controlQueue.GetMessagesAsync(
                        this.settings.ControlQueueBatchSize,
                        this.settings.ControlQueueVisibilityTimeout,
                        ((StorageOrchestrationServiceSettings)this.settings).ControlQueueRequestOptions,
                        null /* operationContext */,
                        cancellationToken);
                    this.stats.StorageRequests.Increment();

                    IEnumerable<MessageData> deserializedBatch = await Task.WhenAll(batch
                        .OfType<CloudQueueMessageWrapper>()
                        .Select(async m => await this.messageManager.DeserializeQueueMessageAsync(m.CloudQueueMessage, controlQueue.Name)));

                    lock (messages)
                    {
                        messages.AddRange(deserializedBatch);
                    }
                });

            return messages;
        }

        


        Task<CloudQueueMessage> CreateOutboundQueueMessageAsync(MessageManager messageManager, TaskMessage taskMessage, string queueName)
        {
            return CreateOutboundQueueMessageInternalAsync(messageManager, this.storageAccountName, this.settings.TaskHubName, queueName, taskMessage);
        }

        async Task<CloudQueueMessage> CreateOutboundQueueMessageInternalAsync(
            MessageManager messageManager,
            string storageAccountName,
            string taskHub,
            string queueName,
            TaskMessage taskMessage)
        {
            // We transfer to a new trace activity ID every time a new outbound queue message is created.
            Guid outboundTraceActivityId = Guid.NewGuid();

            var data = new MessageData(taskMessage, outboundTraceActivityId, queueName);
            string rawContent = await messageManager.SerializeMessageDataAsync(data);

            AnalyticsEventSource.Log.SendingMessage(
                outboundTraceActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                Encoding.Unicode.GetByteCount(rawContent),
                PartitionId: data.QueueName);

            return new CloudQueueMessage(rawContent);
        }


        /// <inheritdoc />
        public async Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions queueRequestOptions)
        {
            CloudQueueMessage message = await CreateOutboundQueueMessageAsync(this.messageManager, taskMessage, queue.Name);
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
            var cloudQueueMessage = await CreateOutboundQueueMessageInternalAsync(
                    this.messageManager,
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    queue.Name,
                    message);

            await wq.cloudQueue.AddMessageAsync(
                cloudQueueMessage,
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
                var queue = queueClient.GetQueueReference(Utils.GetControlQueueId(settings.TaskHubName, i));
                controlQueues[i] = new CloudQueueWrapper(queue);
            }

            return controlQueues;
        }
    }
}
