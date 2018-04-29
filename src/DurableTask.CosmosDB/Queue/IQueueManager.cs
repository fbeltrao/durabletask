using DurableTask.AzureStorage;
using DurableTask.AzureStorage.Monitoring;
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
    /// Queue
    /// </summary>
    interface IQueue
    {
        string Name { get; }

        Task CreateIfNotExistsAsync();

        Task<bool> DeleteIfExistsAsync();

        Task<IEnumerable<CloudQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken);
        Task DeleteMessageAsync(CloudQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext);
        Task UpdateMessageAsync(CloudQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields visibility, QueueRequestOptions requestOptions, OperationContext operationContext);
        Task<CloudQueueMessage> PeekMessageAsync();
        Task<int> GetQueueLenghtAsync();
        Task<bool> ExistsAsync();
    }

    /// <summary>
    /// Queue manager
    /// </summary>
    interface IQueueManager
    {
        /// <summary>
        /// All available control queues
        /// </summary>
        ConcurrentDictionary<string, IQueue> AllControlQueues { get; }

        /// <summary>
        /// Control queues owned
        /// </summary>
        ConcurrentDictionary<string, IQueue> OwnedControlQueues { get; }

        /// <summary>
        /// Work item queue
        /// </summary>
        IQueue WorkItemQueue { get; }

        /// <summary>
        /// Queue persistance name for logging
        /// For Storage: CloudStorageAccount.Credentials.AccountName
        /// </summary>
        string StorageName { get; }

        /// <summary>
        /// Deletes
        /// </summary>
        /// <returns></returns>
        Task DeleteAsync();

        /// <summary>
        /// Starts
        /// </summary>
        /// <returns></returns>
        Task StartAsync();

        /// <summary>
        /// Gets the queue by partition
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        IQueue GetQueue(string partitionId);

        Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Gets the control queue for the partition index
        /// </summary>
        /// <param name="partitionIndex"></param>
        /// <returns></returns>
        IQueue GetControlQueue(int partitionIndex);

        /// <summary>
        /// Gets the control queue for the partition
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        Task<IQueue> GetControlQueueAsync(string partitionId);

        /// <summary>
        /// Enqueues message
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="context"></param>
        /// <param name="taskMessage"></param>
        /// <param name="initialVisibilityDelay"></param>
        /// <param name="requestOptions"></param>
        /// <returns></returns>
        Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions);

        Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext);

        /// <summary>
        /// Gets messages from a <see cref="IQueue"/>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="queueVisibilityTimeout"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken);

        

        /// <summary>
        /// Gets the control queues
        /// </summary>
        /// <param name="partitionCount"></param>
        /// <returns></returns>
        Task<IQueue[]> GetControlQueuesAsync(int partitionCount);
    }
}
