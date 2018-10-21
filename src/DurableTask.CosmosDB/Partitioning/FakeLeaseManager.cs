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

using System.Collections.Generic;
using System.Threading.Tasks;
using DurableTask.AzureStorage.Partitioning;

namespace DurableTask.AzureStorage
{
    internal class FakeLeaseManager : ILeaseManager
    {
        public Task<bool> AcquireAsync(Lease lease, string owner) => Task.FromResult(true);

        public Task CreateLeaseIfNotExistAsync(string partitionId) => Task.FromResult(0);

        public Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo) => Task.FromResult(false);

        public Task DeleteAllAsync() => Task.FromResult(0);

        public Task DeleteAsync(Lease lease) => Task.FromResult(0);

        public Task<Lease> GetLeaseAsync(string partitionId) => Task.FromResult<Lease>(null);

        public Task<bool> LeaseStoreExistsAsync() => Task.FromResult(true);

        public Task<IEnumerable<Lease>> ListLeasesAsync() => Task.FromResult<IEnumerable<Lease>>(null);

        public Task<bool> ReleaseAsync(Lease lease) => Task.FromResult(true);

        public Task<bool> RenewAsync(Lease lease) => Task.FromResult(true);

        public Task<bool> UpdateAsync(Lease lease) => Task.FromResult(true);
    }
}