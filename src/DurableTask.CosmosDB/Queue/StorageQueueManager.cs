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


using DurableTask.CosmosDB.Monitoring;
using DurableTask.CosmosDB.Partitioning;
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
            this.StorageName = account.Credentials.AccountName;
            this.queueClient = account.CreateCloudQueueClient();
            this.queueClient.BufferManager = SimpleBufferManager.Shared;
            this.OwnedControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.AllControlQueues = new ConcurrentDictionary<string, IQueue>();
            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.BufferManager = SimpleBufferManager.Shared;

            string compressedMessageBlobContainerName = $"{settings.TaskHubName.ToLowerInvariant()}-largemessages";
            NameValidator.ValidateContainerName(compressedMessageBlobContainerName);
            this.messageManager = new MessageManager(this.blobClient, compressedMessageBlobContainerName);


            this.WorkItemQueue = this.GetWorkItemQueue(account);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                CloudQueue queue = this.queueClient.GetQueueReference(Utils.GetControlQueueId(this.settings.TaskHubName, i));                
                this.AllControlQueues.TryAdd(queue.Name, new CloudQueueWrapper(queue));
            }
        }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> AllControlQueues { get; }

        /// <inheritdoc />
        public ConcurrentDictionary<string, IQueue> OwnedControlQueues { get; }

        /// <inheritdoc />
        public IQueue WorkItemQueue { get; }

        /// <inheritdoc />
        public string StorageName { get; }

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
            foreach (string partitionId in this.AllControlQueues.Keys)
            {
                if (this.AllControlQueues.TryGetValue(partitionId, out var controlQueue))
                {
                    await controlQueue.DeleteIfExistsAsync();
                }
            }

            await this.WorkItemQueue.DeleteIfExistsAsync();
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            CloudQueue workItemCloudQueue = ((CloudQueueWrapper)this.WorkItemQueue).CloudQueue;
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

            await this.OwnedControlQueues.Values.ParallelForEachAsync(
                async delegate (IQueue controlQueue)
                {
                    IEnumerable<IQueueMessage> batch = await controlQueue.GetMessagesAsync(
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
            return this.CreateOutboundQueueMessageInternalAsync(messageManager, this.StorageName, this.settings.TaskHubName, queueName, taskMessage);
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
            CloudQueueMessage message = await this.CreateOutboundQueueMessageAsync(this.messageManager, taskMessage, queue.Name);
            var wq = (CloudQueueWrapper)queue;
            await wq.CloudQueue.AddMessageAsync(
                message,
                null /* timeToLive */,
                initialVisibilityDelay,
                queueRequestOptions,
                context.StorageOperationContext);
        }

        public async Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var wq = (CloudQueueWrapper)queue;
            CloudQueueMessage queueMessage = await wq.CloudQueue.GetMessageAsync(
                    queueVisibilityTimeout,
                    requestOptions,
                    operationContext,
                    cancellationToken);

            this.stats.StorageRequests.Increment();

            if (queueMessage != null)
            {
                ReceivedMessageContext context = await ReceivedMessageContext.CreateFromReceivedMessageAsync(
                   this.messageManager,
                   this.StorageName,
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
            CloudQueueMessage cloudQueueMessage = await CreateOutboundQueueMessageInternalAsync(
                    this.messageManager,
                    this.StorageName,
                    this.settings.TaskHubName,
                    queue.Name,
                    message);

            await wq.CloudQueue.AddMessageAsync(
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

            BlobLeaseManager inactiveLeaseManager = BlobLeaseManager.GetBlobLeaseManager(this.settings.TaskHubName, "n/a", account, TimeSpan.Zero, TimeSpan.Zero, null);
            TaskHubInfo hubInfo = await inactiveLeaseManager.GetOrCreateTaskHubInfoAsync(
                TaskHubInfo.GetTaskHubInfo(this.settings.TaskHubName, partitionCount));

            CloudQueueClient queueClient = account.CreateCloudQueueClient();

            var controlQueues = new IQueue[hubInfo.PartitionCount];
            for (int i = 0; i < hubInfo.PartitionCount; i++)
            {
                CloudQueue queue = queueClient.GetQueueReference(Utils.GetControlQueueId(settings.TaskHubName, i));
                controlQueues[i] = new CloudQueueWrapper(queue);
            }

            return controlQueues;
        }
    }
}
