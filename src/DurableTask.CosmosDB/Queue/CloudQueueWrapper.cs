using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Wrapper for <see cref="CloudQueue"/>
    /// </summary>
    public class CloudQueueWrapper : IQueue
    {
        internal readonly CloudQueue cloudQueue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cloudQueue"></param>
        public CloudQueueWrapper(CloudQueue cloudQueue)
        {
            this.cloudQueue = cloudQueue;
        }

        /// <inheritdoc />
        public string Name => this.cloudQueue.Name;

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            await this.cloudQueue.CreateIfNotExistsAsync();
        }

        /// <inheritdoc />
        public async Task DeleteMessageAsync(CloudQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            await this.cloudQueue.DeleteMessageAsync(queueMessage, requestOptions, operationContext);
            
        }

        /// <inheritdoc />
        public async Task<IEnumerable<CloudQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions controlQueueRequestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return await this.cloudQueue.GetMessagesAsync(
                controlQueueBatchSize,
                controlQueueVisibilityTimeout,
                controlQueueRequestOptions,
                operationContext,
                cancellationToken
                );
        }

        /// <inheritdoc />
        public async Task UpdateMessageAsync(CloudQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields visibility, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            await this.cloudQueue.UpdateMessageAsync(
                originalQueueMessage,
                controlQueueVisibilityTimeout,
                visibility,
                requestOptions,
                operationContext
                );
        }

        /// <inheritdoc />
        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.cloudQueue.DeleteIfExistsAsync();
        }

        /// <inheritdoc />

        public async Task<CloudQueueMessage> PeekMessageAsync()
        {
            return await cloudQueue.PeekMessageAsync();
        }

        /// <inheritdoc />

        public async Task<int> GetQueueLenghtAsync()
        {
            await cloudQueue.FetchAttributesAsync();
            return cloudQueue.ApproximateMessageCount.GetValueOrDefault(0);
        }

        /// <inheritdoc />

        public async Task<bool> ExistsAsync()
        {
            return await cloudQueue.ExistsAsync();
        }
    }
}
