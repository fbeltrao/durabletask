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
using DurableTask.CosmosDB.Monitoring;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DurableTask.CosmosDB.Partitioning
{
    using DurableTask.CosmosDB.Collection;

    internal partial class CosmosDBLeaseManager : ILeaseManager, IDisposable
    {
        readonly string taskHubName;
        readonly string workerId;
        readonly string cosmosDbName;
        readonly string cosmosDbEndpoint;
        readonly string cosmosDbAuthKey;
        readonly string cosmosDbLeaseManagementCollection;
        readonly TimeSpan leaseInterval;
        TimeSpan leaseRenewInterval;
        AzureStorageOrchestrationServiceStats stats;
        DocumentClient documentClient;

        public CosmosDBLeaseManager(
            string taskHubName, 
            string workerId, 
            string cosmosDBEndpoint, 
            string cosmosDBAuthKey, 
            string cosmosDBName,
            string cosmosDBLeaseManagementCollection, 
            TimeSpan leaseInterval, 
            TimeSpan leaseRenewInterval, 
            AzureStorageOrchestrationServiceStats stats)
        {
            this.taskHubName = taskHubName;
            this.workerId = workerId;
            this.cosmosDbName = cosmosDBName;
            this.cosmosDbEndpoint = cosmosDBEndpoint;
            this.cosmosDbAuthKey = cosmosDBAuthKey;
            this.cosmosDbLeaseManagementCollection = cosmosDBLeaseManagementCollection;
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
                    this.cosmosDbName,
                    this.cosmosDbLeaseManagementCollection);

                ResourceResponse<DocumentCollection> getCollectionResponse = await this.documentClient.ReadDocumentCollectionAsync(collectionUri);
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
                this.stats.CosmosDbRequests.Increment();
            }

            return false;
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo)
        {
            var cosmosDbCollectionDefinition = new CosmosDBCollectionDefinition
            {
                Endpoint = this.cosmosDbEndpoint,
                CollectionName = this.cosmosDbLeaseManagementCollection,
                DbName = this.cosmosDbName,
                SecretKey = this.cosmosDbAuthKey,

            };

            await Utils.CreateCollectionIfNotExists(cosmosDbCollectionDefinition);
            this.stats.CosmosDbRequests.Increment();

            for (int i = 0; i < eventHubInfo.PartitionCount; i++)
            {
                string id = Utils.GetControlQueueId(taskHubName, i);
                await this.CreatePartitionDocumentIfNotExist(id, eventHubInfo.TaskHubName);
            }

            await this.CreateTaskHubInfoIfNotExistAsync(eventHubInfo);

            return false;
        }


        async Task CreatePartitionDocumentIfNotExist(string id, string taskHubName)
        {
            try
            {
                var lease = new CosmosDBLease()
                {
                    PartitionId = id,
                    TaskHubName = taskHubName
                };

                await this.documentClient.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    lease);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != HttpStatusCode.Conflict)
                    throw;
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();

            }
        }

        public async Task<IEnumerable<Lease>> ListLeasesAsync()
        {
            var leases = new List<CosmosDBLease>();
            try
            {

                IDocumentQuery<CosmosDBLease> feed = this.documentClient.CreateDocumentQuery<CosmosDBLease>(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection))
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
                this.stats.CosmosDbRequests.Increment();
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
                    this.cosmosDbLeaseManagementCollection,
                    this.taskHubName,
                    this.workerId,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - collectionName: {0}, taskHubName: {1}, partitionId: {2}",
                        this.cosmosDbLeaseManagementCollection,
                        this.taskHubName,
                        partition));

                await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    lease);
            }
            catch (DocumentClientException documentClientException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.cosmosDbLeaseManagementCollection,
                    this.taskHubName,
                    this.workerId,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - collectionName: {0}, taskHubName: {1}, partitionId: {2}, exception: {3}.",
                        this.cosmosDbLeaseManagementCollection,
                        this.taskHubName,
                        partition,
                        documentClientException));
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
            }
        }

        public async Task<Lease> GetLeaseAsync(string partitionId)
        {
            try
            {
                ResourceResponse<Document> res = await this.documentClient.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection, partitionId));

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

            var cosmosDbLease = ((CosmosDBLease)lease);
            try
            {
                var desiredLeaseState = new CosmosDBLease(cosmosDbLease)
                {
                    LeaseTimeout = Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)),
                    Epoch = cosmosDbLease.Epoch + 1,
                };

                ResourceResponse<Document> res = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDbLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

                cosmosDbLease.Token = res.Resource.ETag;
                cosmosDbLease.Epoch = desiredLeaseState.Epoch;
                cosmosDbLease.LeaseTimeout = desiredLeaseState.LeaseTimeout;
            }
            catch (DocumentClientException ex)
            {
                throw HandleDocumentClientException(lease, ex);
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
            }

            return true;
        }

        public async Task<bool> AcquireAsync(Lease lease, string owner)
        {
            var cosmosDbLease = ((CosmosDBLease)lease);
            try
            {

                var desiredLeaseState = new CosmosDBLease(cosmosDbLease)
                {
                    LeaseTimeout = Utils.ToUnixTime(DateTime.UtcNow.Add(this.leaseInterval)),
                    Owner = owner,
                    Epoch = cosmosDbLease.Epoch + 1,
                };

                ResourceResponse<Document> updateResponse = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDbLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });

                if (updateResponse.StatusCode == HttpStatusCode.OK)
                {
                    cosmosDbLease.Token = updateResponse.Resource.ETag;
                    cosmosDbLease.Owner = owner;
                    cosmosDbLease.LeaseTimeout = desiredLeaseState.LeaseTimeout;
                    cosmosDbLease.Epoch = desiredLeaseState.Epoch;
                }
            }
            catch (DocumentClientException documentClientException)
            {
                throw HandleDocumentClientException(lease, documentClientException);
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();

            }

            return true;
        }

        public async Task<bool> ReleaseAsync(Lease lease)
        {
            var cosmosDbLease = ((CosmosDBLease)lease);
            try
            {
                var desiredLeaseState = new CosmosDBLease(cosmosDbLease)
                {
                    Owner = null,
                    LeaseTimeout = 0,
                    Epoch = cosmosDbLease.Epoch + 1
                };


                ResourceResponse<Document> res = await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    desiredLeaseState,
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDbLease.Token,
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
                this.stats.CosmosDbRequests.Increment();
            }

            return true;
        }

        public async Task DeleteAsync(Lease lease)
        {
            var cosmosDbLease = ((CosmosDBLease)lease);
            try
            {
                await this.documentClient.DeleteDocumentAsync(
                    UriFactory.CreateDocumentUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection, cosmosDbLease.Id),
                    new RequestOptions
                    {
                        AccessCondition = new AccessCondition
                        {
                            Condition = cosmosDbLease.Token,
                            Type = AccessConditionType.IfMatch
                        }
                    });
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
            }
        }

        public async Task DeleteAllAsync()
        {
            try
            {
                // for now just loop through leases and delete them
                // Better solution would be to call a stored procedure that will do it in a single batch
                foreach (Lease lease in await this.ListLeasesAsync())
                {
                    await this.DeleteAsync(lease);
                }
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
            }
        }

        public async Task<bool> UpdateAsync(Lease lease)
        {
            if (string.IsNullOrWhiteSpace(lease?.Token))
            {
                return false;
            }

            var leaseBlob = (CosmosDBLease)lease;
            try
            {
                await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
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
                this.stats.CosmosDbRequests.Increment();
            }

            return true;
        }

        string GetTaskHubInfoDocumentId()
        {
            return string.Concat(this.taskHubName, "-", "taskhubinfo");
        }

        public class CosmosDbTaskHubInfoWrapper
        {
            public string id { get; set; }

            public CosmosDbTaskHubInfoWrapper()
            {                
            }

            public CosmosDbTaskHubInfoWrapper(string id, TaskHubInfo taskHubInfo)
            {
                this.id = id;
                this.TaskHubInfo = taskHubInfo;
            }

            public TaskHubInfo TaskHubInfo { get; set;  }
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
        {
            try
            {
                await this.documentClient.UpsertDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection),
                    new CosmosDbTaskHubInfoWrapper(this.GetTaskHubInfoDocumentId(), taskHubInfo));
            }
            catch (DocumentClientException)
            {
                // eat any document client exception related to conflict
                // this means the document already exist
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
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
            this.documentClient = new DocumentClient(new Uri(this.cosmosDbEndpoint), this.cosmosDbAuthKey);
        }

        async Task<TaskHubInfo> GetTaskHubInfoAsync()
        {
            try
            {
                DocumentResponse<DocumentResponse<CosmosDbTaskHubInfoWrapper>> res = await this.documentClient.ReadDocumentAsync<DocumentResponse<CosmosDbTaskHubInfoWrapper>>(
                         UriFactory.CreateDocumentUri(this.cosmosDbName, this.cosmosDbLeaseManagementCollection, GetTaskHubInfoDocumentId()));
                return res.Document?.Document?.TaskHubInfo;
            }
            catch (DocumentClientException documentClientException)
            {
                if (documentClientException.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }
            finally
            {
                this.stats.CosmosDbRequests.Increment();
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

        public void Dispose()
        {
            this.documentClient?.Dispose();
        }
    }
}