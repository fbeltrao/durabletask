using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Sql server queue settings
    /// </summary>
    public class SqlServerQueueSettings
    {
        /// <summary>
        /// Connection string
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets/sets if memory optimized table should be used for queues
        /// </summary>
        public bool UseMemoryOptimizedTable { get; internal set; }
    }
}
