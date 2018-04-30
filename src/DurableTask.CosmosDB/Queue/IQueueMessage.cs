using System;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Represents a queue message
    /// </summary>
    public interface IQueueMessage
    {
        /// <summary>
        /// Insert time
        /// </summary>
        DateTimeOffset? InsertionTime { get; }

        /// <summary>
        /// Identifier
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Dequeue count 
        /// </summary>
        int DequeueCount { get;  }

        /// <summary>
        /// Next visible time
        /// </summary>
        DateTimeOffset? NextVisibleTime { get; }

        /// <summary>
        /// Message as string
        /// </summary>
        /// <returns></returns>
        string AsString();
    }
}
