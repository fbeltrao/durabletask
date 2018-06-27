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

namespace DurableTask.CosmosDB.Partitioning
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    interface ILeaseManager
    {
        Task<bool> LeaseStoreExistsAsync();

        Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo);

        Task<IEnumerable<Lease>> ListLeasesAsync();

        Task CreateLeaseIfNotExistAsync(string partitionId);

        Task<Lease> GetLeaseAsync(string partitionId);

        Task<bool> RenewAsync(Lease lease);

        Task<bool> AcquireAsync(Lease lease, string owner);

        Task<bool> ReleaseAsync(Lease lease);

        Task DeleteAsync(Lease lease);

        Task DeleteAllAsync();

        Task<bool> UpdateAsync(Lease lease);
    }
}
