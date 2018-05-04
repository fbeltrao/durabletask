using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
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

        Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken);
        Task DeleteMessageAsync(IQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext);
        Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields visibility, QueueRequestOptions requestOptions, OperationContext operationContext);
        Task<IQueueMessage> PeekMessageAsync();
        Task<int> GetQueueLenghtAsync();
        Task<bool> ExistsAsync();
    }
}
