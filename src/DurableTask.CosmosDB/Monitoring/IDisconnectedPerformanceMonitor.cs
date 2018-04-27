using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Monitoring
{
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;

    /// <summary>
    /// Disconnected Performance Monitory
    /// </summary>
    public interface IDisconnectedPerformanceMonitor
    {
        /// <summary>
        /// Collects and returns a sampling of all performance metrics being observed by this instance.
        /// </summary>
        /// <param name="currentWorkerCount">The number of workers known to be processing messages for this task hub.</param>
        /// <returns>Returns a performance data summary or <c>null</c> if data cannot be obtained.</returns>
         Task<PerformanceHeartbeat> PulseAsync(int currentWorkerCount);

    }
}
