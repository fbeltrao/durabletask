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

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DurableTask.AzureStorage.Monitoring;
using DurableTask.AzureStorage.Partitioning;
using DurableTask.CosmosDB;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DurableTask.AzureStorage
{
    internal partial class SqlLeaseManager : ILeaseManager
    {
        private string taskHubName;
        private string workerId;
        private readonly string sqlConnectionString;
        private TimeSpan leaseInterval;
        private TimeSpan leaseRenewInterval;
        private AzureStorageOrchestrationServiceStats stats;

        public SqlLeaseManager(
            string taskHubName, 
            string workerId, 
            string sqlConnectionString,
            TimeSpan leaseInterval, 
            TimeSpan leaseRenewInterval, 
            AzureStorageOrchestrationServiceStats stats)
        {
            this.taskHubName = taskHubName;
            this.workerId = workerId;
            this.sqlConnectionString = sqlConnectionString;
            this.leaseInterval = leaseInterval;
            this.leaseRenewInterval = leaseRenewInterval;
            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

            this.Initialize();
        }


        public Task<bool> LeaseStoreExistsAsync()
        {
            // TODO: implement
            return Task.FromResult(true);

        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo taskHubInfo)
        {
            bool result = false;
          
            for (int i = 0; i < taskHubInfo.PartitionCount; i++)
            {
                var id = Utils.GetControlQueueId(taskHubName, i);
                await CreatePartitionDocumentIfNotExist(id, taskHubInfo.TaskHubName);
            }

            await this.CreateTaskHubInfoIfNotExistAsync(taskHubInfo);

            return result;
        }

        SqlConnection GetConnection() => new SqlConnection(this.sqlConnectionString);

        private async Task CreatePartitionDocumentIfNotExist(string partitionId, string taskHubName, string owner = null)
        {
            try
            {
                var lease = new SqlLease()
                {
                    PartitionId = partitionId,
                    TaskHubName = taskHubName
                };

                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand("up_lease_Create", conn)
                    {
                        CommandType = System.Data.CommandType.StoredProcedure
                    };
                    
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@partitionId", partitionId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", taskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                    if (!string.IsNullOrEmpty(owner))
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@owner", owner) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                    await conn.OpenAsync();

                    await cmd.ExecuteNonQueryAsync();
                }
                
            }            
            finally
            {
                this.stats.CosmosDBRequests.Increment();

            }
        }

        public async Task<IEnumerable<Lease>> ListLeasesAsync()
        {
            var leases = new List<SqlLease>();
            try
            {
                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand("up_lease_List", conn)
                    {
                        CommandType = System.Data.CommandType.StoredProcedure
                    };
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", taskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                    await conn.OpenAsync();

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var l = new SqlLease()
                            {
                                PartitionId = reader.GetString(0),
                                Epoch = reader.GetInt64(1),
                                LeaseTimeout = reader.GetInt64(2),
                                Owner = reader.GetString(3),
                                TaskHubName = reader.GetString(4),
                                Token = reader.GetInt64(5).ToString()
                            };

                            leases.Add(l);
                        }
                    }
                }                    
            }            
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return leases.OrderBy(x => x.PartitionId);
        }

        public async Task CreateLeaseIfNotExistAsync(string partition)
        {            
            try
            {
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    nameof(SqlLeaseManager),
                    this.taskHubName,
                    this.workerId,
                    $"{nameof(CreateLeaseIfNotExistAsync)} = {nameof(SqlLeaseManager)} taskHubName: {taskHubName}, partitionId: {partition}");

                await CreatePartitionDocumentIfNotExist(partition, this.taskHubName, this.workerId);
                
            }
            catch (Exception exception)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    nameof(SqlLeaseManager),
                    this.taskHubName,
                    this.workerId,
                    $"{nameof(CreateLeaseIfNotExistAsync)} = {nameof(SqlLeaseManager)} taskHubName: {taskHubName}, partitionId: {partition}, exception {exception}");
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
        }

        public async Task<Lease> GetLeaseAsync(string partitionId)
        {
            using (var conn = GetConnection())
            {
                var cmd = new SqlCommand("up_lease_Get", conn)
                {
                    CommandType = System.Data.CommandType.StoredProcedure
                };
                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@partitionId", partitionId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", taskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                await conn.OpenAsync();

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var l = new SqlLease()
                        {
                            PartitionId = reader.GetString(0),
                            Epoch = reader.GetInt64(1),
                            LeaseTimeout = reader.GetInt64(2),
                            Owner = reader.GetString(3),
                            TaskHubName = reader.GetString(4),
                            Token = reader.GetInt64(5).ToString()
                        };

                        return l;
                    }
                }
            }

            return null;
        }

        public async Task<bool> RenewAsync(Lease lease)
        {

            var sqlLease = ((SqlLease)lease);
            return await InternalUpdateLease(lease,
                timeout: Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)));
        }

        async Task<bool> InternalUpdateLease(Lease lease, long? timeout = null, string owner = null, long? epoch = null)
        {
            var sqlLease = ((SqlLease)lease);
            try
            {

                var succeeded = false;
                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand("up_lease_UpdateIf", conn)
                    {
                        CommandType = System.Data.CommandType.StoredProcedure
                    };
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@partitionId", lease.PartitionId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", taskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@token", sqlLease.Token) { SqlDbType = System.Data.SqlDbType.BigInt });

                    if (timeout.HasValue)
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@leaseTimeout", timeout.Value) { SqlDbType = System.Data.SqlDbType.BigInt });

                    if (owner != null)
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@owner", owner) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                    if (owner != null)
                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@epoch", epoch.Value) { SqlDbType = System.Data.SqlDbType.BigInt });

                    await conn.OpenAsync();

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            sqlLease.Epoch = reader.GetInt64(0);
                            sqlLease.LeaseTimeout = reader.GetInt64(1);
                            sqlLease.Owner = reader.GetString(2);
                            sqlLease.TaskHubName = reader.GetString(3);
                            sqlLease.Token = reader.GetInt64(4).ToString();

                            succeeded = true;
                        }
                    }
                }

                if (!succeeded)
                    throw new LeaseLostException(lease);

            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();

            }

            return true;
        }

        public async Task<bool> AcquireAsync(Lease lease, string owner)
        {
            var sqlLease = ((SqlLease)lease);
            return await InternalUpdateLease(lease,
                timeout: Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)),
                owner: owner,
                epoch: 1);
        }

        public async Task<bool> ReleaseAsync(Lease lease)
        {
            var sqlLease = ((SqlLease)lease);
            return await InternalUpdateLease(lease,
                timeout: 0,
                owner: string.Empty,
                epoch: 0);
        }

        public Task DeleteAsync(Lease lease)
        {
            // TODO: implement
            return Task.FromResult(0);
        }

        public async Task DeleteAllAsync()
        {
            try
            {
                // for now just loop through leases and delete them
                // Better solution would be to call a stored procedure that will do it in a single batch
                foreach (var lease in await this.ListLeasesAsync())
                {
                    await DeleteAsync(lease);
                }
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
        }

        public async Task<bool> UpdateAsync(Lease lease)
        {
            if (lease == null || string.IsNullOrWhiteSpace(lease.Token))
            {
                return false;
            }

            var sqlLease = ((SqlLease)lease);
            return await InternalUpdateLease(lease);
            
        }

        string GetTaskHubInfoDocumentId()
        {
            return string.Concat(this.taskHubName, "-", "taskhubinfo");
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
        {
            try
            {
                using (var conn = GetConnection())
                {
                    var cmd = new SqlCommand("up_taskHub_Create", conn)
                    {
                        CommandType = System.Data.CommandType.StoredProcedure
                    };
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", taskHubInfo.TaskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@partitionCount", taskHubInfo.PartitionCount) { SqlDbType = System.Data.SqlDbType.Int });

                    await conn.OpenAsync();

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
        }

        internal async Task<TaskHubInfo> GetOrCreateTaskHubInfoAsync(TaskHubInfo createdTaskHubInfo)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
            if (currentTaskHubInfo != null)
            {
                return currentTaskHubInfo;
            }

            await this.CreateTaskHubInfoIfNotExistAsync(createdTaskHubInfo);
            return createdTaskHubInfo;
        }

        internal async Task<bool> IsStaleLeaseStore(TaskHubInfo taskHubInfo)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
            if (currentTaskHubInfo != null)
            {
                if (!currentTaskHubInfo.TaskHubName.Equals(taskHubInfo.TaskHubName, StringComparison.OrdinalIgnoreCase)
                    || !currentTaskHubInfo.CreatedAt.Equals(taskHubInfo.CreatedAt)
                    || !currentTaskHubInfo.PartitionCount.Equals(taskHubInfo.PartitionCount))
                {
                    return true;
                }
            }

            return false;
        }

        void Initialize()
        {
            
        }

        async Task<TaskHubInfo> GetTaskHubInfoAsync()
        {
            try
            {

                try
                {
                    using (var conn = GetConnection())
                    {
                        var cmd = new SqlCommand("up_taskHub_Get", conn)
                        {
                            CommandType = System.Data.CommandType.StoredProcedure
                        };

                        cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@taskHubName", this.taskHubName) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });

                        await conn.OpenAsync();

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                return new TaskHubInfo(reader.GetString(0), reader.GetDateTime(1), reader.GetInt32(2));
                            }
                        }   
                    }

                    return null;
                }
                finally
                {
                    this.stats.CosmosDBRequests.Increment();
                }
            }            
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

        }
    }
}