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
