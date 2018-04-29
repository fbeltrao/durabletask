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
    internal partial class CosmosDBLeaseManager : ILeaseManager
    {
        private string taskHubName;
        private string workerId;
        private readonly string cosmosDBName;
        private string cosmosDBEndpoint;
        private string cosmosDBAuthKey;
        private string cosmosDBLeaseManagementCollection;
        private TimeSpan leaseInterval;
        private TimeSpan leaseRenewInterval;
        private AzureStorageOrchestrationServiceStats stats;
        private DocumentClient documentClient;

        public CosmosDBLeaseManager(
            string taskHubName, 
            string workerId, 
            string cosmosDBEndpoint, 
            string cosmosDBAuthKey, 
            string cosmosDBLeaseManagementCollection, 
            TimeSpan leaseInterval, 
            TimeSpan leaseRenewInterval, 
            AzureStorageOrchestrationServiceStats stats)
        {
            this.taskHubName = taskHubName;
            this.workerId = workerId;
            this.cosmosDBName = "durabletask";
            this.cosmosDBEndpoint = cosmosDBEndpoint;
            this.cosmosDBAuthKey = cosmosDBAuthKey;
            this.cosmosDBLeaseManagementCollection = cosmosDBLeaseManagementCollection;
            this.leaseInterval = leaseInterval;
            this.leaseRenewInterval = leaseRenewInterval;
            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

            this.Initialize();
        }
        

        public async Task<bool> LeaseStoreExistsAsync()
        {
            try
            {
                var collectionUri = UriFactory.CreateDocumentCollectionUri(
                    this.cosmosDBName, 
                    this.cosmosDBLeaseManagementCollection);

                var getCollectionResponse = await this.documentClient.ReadDocumentCollectionAsync(collectionUri);
                if (getCollectionResponse.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch (DocumentClientException documentClientException)
            {
                if (documentClientException.StatusCode != System.Net.HttpStatusCode.NotFound)
                    throw;
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return false;
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo)
        {
            bool result = false;

            var cosmosDbCollectionDefinition = new CosmosDBCollectionDefinition
            {
                Endpoint = this.cosmosDBEndpoint,
                CollectionName = this.cosmosDBLeaseManagementCollection,
                DbName = cosmosDBName,
                SecretKey = this.cosmosDBAuthKey,

            };

            await Utils.CreateCollectionIfNotExists(cosmosDbCollectionDefinition);
            this.stats.CosmosDBRequests.Increment();

            for (int i = 0; i < eventHubInfo.PartitionCount; i++)
            {
                var id = $"{taskHubName}-control-{i.ToString("00")}".ToLowerInvariant();
                await CreatePartitionDocumentIfNotExist(id, eventHubInfo.TaskHubName);
            } 

            await this.CreateTaskHubInfoIfNotExistAsync(eventHubInfo);

            return result;
        }


        private async Task CreatePartitionDocumentIfNotExist(string id, string taskHubName)
        {
            try
            {
                var lease = new CosmosDBLease()
                {
                    PartitionId = id,
                    TaskHubName = taskHubName
                };

                await this.documentClient.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDBName, this.cosmosDBLeaseManagementCollection),
                    lease);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != HttpStatusCode.Conflict)
                    throw;
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();

            }
        }

        public async Task<IEnumerable<Lease>> ListLeasesAsync()
        {
            var leases = new List<CosmosDBLease>();
            try
            {

                var feed = this.documentClient.CreateDocumentQuery<CosmosDBLease>(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDBName, this.cosmosDBLeaseManagementCollection))
                    .Where(d => d.TaskHubName == taskHubName)
                    .AsDocumentQuery();

                while (feed.HasMoreResults)
                {
                    FeedResponse<Document> items = await feed.ExecuteNextAsync<Document>();
                    foreach (var f in items)
                    {
                        var l = JsonConvert.DeserializeObject<CosmosDBLease>(f.ToString());
                        l.Token = f.ETag;
                        leases.Add(l);
                    }
                }
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
            
            return leases.OrderBy(x => x.PartitionId);
        }

        public async Task CreateLeaseIfNotExistAsync(string partition)
        {
            var lease = new CosmosDBLease()
            {
                PartitionId = partition,
                Owner = this.workerId,
                TaskHubName = this.taskHubName,
                Epoch = 0
            };

            try
            {
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.cosmosDBLeaseManagementCollection,
                    this.taskHubName,
                    this.workerId,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - collectionName: {0}, taskHubName: {1}, partitionId: {2}",
                        this.cosmosDBLeaseManagementCollection,
                        this.taskHubName,
                        partition));

                await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBLeaseManagementCollection),
                    lease);
            }
            catch (DocumentClientException documentClientException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.cosmosDBLeaseManagementCollection,
                    this.taskHubName,
                    this.workerId,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - collectionName: {0}, taskHubName: {1}, partitionId: {2}, exception: {3}.",
                        this.cosmosDBLeaseManagementCollection,
                        this.taskHubName,
                        partition,
                        documentClientException));
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
        }

        private string GetPartitionId(string partition)
        {
            return string.Concat(this.taskHubName, "-", partition);
        }

        public async Task<Lease> GetLeaseAsync(string partitionId)
        {
            try
            {
                var res = await this.documentClient.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(cosmosDBName, cosmosDBLeaseManagementCollection, GetPartitionId(partitionId)));

                var lease = (CosmosDBLease)(dynamic)res.Resource;
                lease.Token = res.Resource.ETag;

                return lease;
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != System.Net.HttpStatusCode.NotFound)
                    throw;
            }
            
            return null;
        }

        public async Task<bool> RenewAsync(Lease lease)
        {
            
            var cosmosDBLease = ((CosmosDBLease)lease);
            try
            {
                var desiredLeaseState = new CosmosDBLease(cosmosDBLease)
                {
                    LeaseTimeout = Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)),
                    Epoch = cosmosDBLease.Epoch + 1,
                };

                var res = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDBLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

                cosmosDBLease.Token = res.Resource.ETag;
                cosmosDBLease.Epoch = desiredLeaseState.Epoch;
                cosmosDBLease.LeaseTimeout = desiredLeaseState.LeaseTimeout;
            }
            catch (DocumentClientException ex)
            {
                throw HandleDocumentClientException(lease, ex);
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return true;
        }

        public async Task<bool> AcquireAsync(Lease lease, string owner)
        {
            var cosmosDBLease = ((CosmosDBLease)lease);
            try
            {

                var desiredLeaseState = new CosmosDBLease(cosmosDBLease)
                {
                    LeaseTimeout = Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)),
                    Owner = owner,
                    Epoch = cosmosDBLease.Epoch + 1,
                };

                var updateResponse = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDBLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

                if (updateResponse.StatusCode == HttpStatusCode.OK)
                {
                    cosmosDBLease.Token = updateResponse.Resource.ETag;
                    cosmosDBLease.Owner = owner;
                    cosmosDBLease.LeaseTimeout = desiredLeaseState.LeaseTimeout;
                    cosmosDBLease.Epoch = desiredLeaseState.Epoch;
                }
            }
            catch (DocumentClientException documentClientException)
            {
                throw HandleDocumentClientException(lease, documentClientException);
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();

            }

            return true;
        }

        public async Task<bool> ReleaseAsync(Lease lease)
        {
            var cosmosDBLease = ((CosmosDBLease)lease);
            try
            {
                var desiredLeaseState = new CosmosDBLease(cosmosDBLease)
                {
                    Owner = null,
                    LeaseTimeout = 0,
                    Epoch = cosmosDBLease.Epoch + 1
                };


                var res = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDBLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

                lease.Token = res.Resource.ETag;
                lease.Owner = null;
                lease.Epoch = desiredLeaseState.Epoch;

            }
            catch (DocumentClientException documentClientException)
            {
                throw HandleDocumentClientException(lease, documentClientException);
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return true;
        }

        public async Task DeleteAsync(Lease lease)
        {
            var cosmosDBLease = ((CosmosDBLease)lease);
            try
            {
                await this.documentClient.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(cosmosDBName, cosmosDBLeaseManagementCollection, cosmosDBLease.Id),
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDBLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }
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

            var leaseBlob = (CosmosDBLease)lease;
            try
            {
                await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBLeaseManagementCollection),
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = lease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

         
            }
            catch (DocumentClientException documentClientException)
            {
                throw HandleDocumentClientException(lease, documentClientException);
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return true;
        }
        
        string GetTaskHubInfoDocumentId()
        {
            return string.Concat(this.taskHubName, "-", "taskhubinfo");
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
        {
            try
            {
                await this.documentClient.CreateDocumentAsync(
                    UriFactory.CreateDocumentUri(cosmosDBName, cosmosDBLeaseManagementCollection, GetTaskHubInfoDocumentId()),
                    taskHubInfo);
            }
            catch (DocumentClientException)
            {
                // eat any document client exception related to conflict
                // this means the document already exist
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
            this.documentClient = new DocumentClient(new Uri(this.cosmosDBEndpoint), this.cosmosDBAuthKey);      
        }

        async Task<TaskHubInfo> GetTaskHubInfoAsync()
        {
            try
            {
                var res = await this.documentClient.ReadDocumentAsync<DocumentResponse<TaskHubInfo>>(
                         UriFactory.CreateDocumentUri(cosmosDBName, cosmosDBLeaseManagementCollection, GetTaskHubInfoDocumentId()));
                return res.Document.Document;
            }
            catch (DocumentClientException documentClientException)
            {
                if (documentClientException.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }
            finally
            {
                this.stats.CosmosDBRequests.Increment();
            }

            return null;
        }


        static Exception HandleDocumentClientException(Lease lease, DocumentClientException exception, bool ignoreLeaseLost = false)
        {
            if (exception.StatusCode == HttpStatusCode.Conflict || exception.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                // Don't throw LeaseLostException if caller chooses to ignore it.
                if (!ignoreLeaseLost)
                {
                    return new LeaseLostException(lease, exception);
                }
            }

            return exception;
        }
    }
}