using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    /// <summary>
    /// Data structure containing scale-related statistics for the work-item queue.
    /// </summary>
    public struct WorkItemQueueData
    {
        /// <summary>
        /// Gets or sets the number of messages in the work-item queue.
        /// </summary>
        public int QueueLength { get; internal set; }

        /// <summary>
        /// Gets or sets the age of the first message in the work-item queue.
        /// </summary>
        public TimeSpan FirstMessageAge { get; internal set; }
    }
}
