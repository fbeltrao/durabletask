﻿using DurableTask.AzureStorage;
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

        Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Gets the control queue for the partition index
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        IQueue GetControlQueue(string id);

        ///// <summary>
        ///// Gets the control queue for the partition
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //Task<IQueue> GetControlQueueAsync(string id);

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