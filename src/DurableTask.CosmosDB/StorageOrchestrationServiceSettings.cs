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

namespace DurableTask.CosmosDB
{
    using System;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Settings that impact the runtime behavior of the <see cref="ExtensibleOrchestrationService"/>.
    /// </summary>
    public class StorageOrchestrationServiceSettings : IExtensibleOrchestrationServiceSettings
    {
        /// <inheritdoc />
        public int ControlQueueBatchSize { get; set; } = 32;

        /// <inheritdoc />
        public int ControlQueueBufferThreshold { get; set; } = 64;

        /// <inheritdoc />
        public TimeSpan ControlQueueVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(90);

        /// <inheritdoc />
        public QueueRequestOptions ControlQueueRequestOptions { get; set; }

        /// <inheritdoc />
        public TimeSpan WorkItemQueueVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(90);

        /// <summary>
        /// Gets or sets the <see cref="QueueRequestOptions"/> that are provided to all internal 
        /// usage of <see cref="CloudQueue"/> APIs for the work item queue.
        /// </summary>
        public QueueRequestOptions WorkItemQueueRequestOptions { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="TableRequestOptions"/> that are provided to all internal
        /// usage of the <see cref="CloudTable"/> APIs for the history table.
        /// </summary>
        public TableRequestOptions HistoryTableRequestOptions { get; set; }

        /// <inheritdoc />
        public string StorageConnectionString { get; set; }

        /// <inheritdoc />
        public string TaskHubName { get; set; }

        /// <inheritdoc />
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 10;

        /// <inheritdoc />
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <inheritdoc />
        public string WorkerId { get; set; } = Environment.MachineName;

        /// <inheritdoc />
        public int PartitionCount { get; set; } = Utils.DefaultPartitionCount;

        /// <inheritdoc />
        public TimeSpan LeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <inheritdoc />
        public TimeSpan LeaseAcquireInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <inheritdoc />
        public TimeSpan LeaseInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <inheritdoc />
        public string CosmosDBEndpoint { get; set; }

        /// <inheritdoc />
        public string CosmosDBAuthKey { get; set; }

        /// <inheritdoc />
        public string CosmosDBLeaseManagementCollection { get; set; }

        /// <inheritdoc />
        public string CosmosDBName { get; set; } = "durabletask";
    }
}
