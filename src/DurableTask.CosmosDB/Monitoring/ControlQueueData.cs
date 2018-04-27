using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    /// <summary>
    /// Data structure containing the number of partitions and the aggregate
    /// number of messages across those control queue partitions.
    /// </summary>
    public struct ControlQueueData
    {
        /// <summary>
        /// Gets or sets the number of control queue partitions.
        /// </summary>
        public int PartitionCount { get; internal set; }

        /// <summary>
        /// Gets or sets the number of messages across all control queues.
        /// </summary>
        public int AggregateQueueLength { get; internal set; }
    }
}
