using System;

namespace DurableTask.CosmosDB
{
    /// <summary>
    /// Extensible Orchestration ServiceSettings Interface
    /// </summary>
    public interface IExtensibleOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the number of messages to pull from the control queue at a time. The default is 32.
        /// The maximum batch size supported by Azure Storage Queues is 32.
        /// </summary>
        int ControlQueueBatchSize { get; set; }

        /// <summary>
        /// Gets or sets the number of control queue messages that can be buffered in memory at a time, at which
        /// point the dispatcher will wait before dequeuing any additional messages. The default is 64.
        /// </summary>
        int ControlQueueBufferThreshold { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout of dequeued control queue messages. The default is 90 seconds.
        /// </summary>
        TimeSpan ControlQueueVisibilityTimeout { get; set; }

        /// <summary>
        /// /// Gets or sets the visibility timeout of dequeued work item queue messages. The default is 90 seconds.
        /// </summary>
        TimeSpan WorkItemQueueVisibilityTimeout { get; set; }

        /// <summary>
        /// Gets or sets the Azure Storage connection string.
        /// </summary>
        string StorageConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the task hub. This value is used to group related storage resources.
        /// </summary>
        string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 10.
        /// </summary>
        int MaxConcurrentTaskActivityWorkItems { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        int MaxConcurrentTaskOrchestrationWorkItems { get; set; }

        /// <summary>
        /// Gets or sets the identifier for the current worker.
        /// </summary>
        string WorkerId { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of orchestration partitions.
        /// </summary>
        int PartitionCount { get; set; }

        /// <summary>
        /// Renew interval for all leases for partitions currently held.
        /// </summary>
        TimeSpan LeaseRenewInterval { get; set; }

        /// <summary>
        /// Interval when the current worker instance kicks off a task to compute if partitions are distributed evenly.
        /// among known host instances. 
        /// </summary>
        TimeSpan LeaseAcquireInterval { get; set; }

        /// <summary>
        /// Interval for which the lease is taken on Azure Blob representing a task hub partition.  If the lease is not renewed within this 
        /// interval, it will cause it to expire and ownership of the partition will move to another worker instance.
        /// </summary>
        TimeSpan LeaseInterval { get; set; } 

        /// <summary>
        /// CosmosDB endpoint
        /// </summary>
        string CosmosDBEndpoint { get; set; }

        /// <summary>
        /// CosmosDB authorization key
        /// </summary>
        string CosmosDBAuthKey { get; set; }
    }
}
