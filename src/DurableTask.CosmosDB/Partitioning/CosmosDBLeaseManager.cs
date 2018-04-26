////  ----------------------------------------------------------------------------------
////  Copyright Microsoft Corporation
////  Licensed under the Apache License, Version 2.0 (the "License");
////  you may not use this file except in compliance with the License.
////  You may obtain a copy of the License at
////  http://www.apache.org/licenses/LICENSE-2.0
////  Unless required by applicable law or agreed to in writing, software
////  distributed under the License is distributed on an "AS IS" BASIS,
////  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
////  See the License for the specific language governing permissions and
////  limitations under the License.
////  ----------------------------------------------------------------------------------

//using System;
//using System.Collections.Generic;
//using System.Globalization;
//using System.Linq;
//using System.Threading.Tasks;
//using DurableTask.AzureStorage.Monitoring;
//using DurableTask.AzureStorage.Partitioning;
//using DurableTask.CosmosDB;
//using Microsoft.Azure.Documents;
//using Microsoft.Azure.Documents.Client;

//namespace DurableTask.AzureStorage
//{
//    internal class CosmosDBLeaseManager : ILeaseManager
//    {
//        const string TaskHubInfoBlobName = "taskhub.json";
//        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);

//        private string taskHubName;
//        private string workerId;
//        private readonly string cosmosDBName;
//        private readonly string GetLeaseStoredProcedureName;
//        private string cosmosDBEndpoint;
//        private string cosmosDBAuthKey;
//        private string cosmosDBLeaseManagementCollection;
//        private TimeSpan leaseInterval;
//        private TimeSpan leaseRenewInterval;
//        private AzureStorageOrchestrationServiceStats stats;
//        private DocumentClient documentClient;

//        public CosmosDBLeaseManager(
//            string taskHubName, 
//            string workerId, 
//            string cosmosDBEndpoint, 
//            string cosmosDBAuthKey, 
//            string cosmosDBLeaseManagementCollection, 
//            TimeSpan leaseInterval, 
//            TimeSpan leaseRenewInterval, 
//            AzureStorageOrchestrationServiceStats stats)
//        {
//            this.taskHubName = taskHubName;
//            this.workerId = workerId;
//            this.cosmosDBName = "durabletask";
//            this.cosmosDBEndpoint = cosmosDBEndpoint;
//            this.cosmosDBAuthKey = cosmosDBAuthKey;
//            this.cosmosDBLeaseManagementCollection = cosmosDBLeaseManagementCollection;
//            this.leaseInterval = leaseInterval;
//            this.leaseRenewInterval = leaseRenewInterval;
//            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

//            this.Initialize();
//        }
        

//        public async Task<bool> LeaseStoreExistsAsync()
//        {
//            try
//            {
//                var collectionUri = UriFactory.CreateDocumentCollectionUri(
//                    this.cosmosDBName, 
//                    this.cosmosDBLeaseManagementCollection);

//                var getCollectionResponse = await this.documentClient.ReadDocumentCollectionAsync(collectionUri);
//                if (getCollectionResponse.StatusCode == System.Net.HttpStatusCode.OK)
//                {
//                    return true;
//                }
//            }
//            catch (DocumentClientException documentClientException)
//            {
//                if (documentClientException.StatusCode != System.Net.HttpStatusCode.NotFound)
//                    throw;
//            }
//            finally
//            {
//                this.stats.CosmosDBRequests.Increment();
//            }

//            return false;
//        }

//        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo)
//        {
//            bool result = false;

//            var cosmosDbCollectionDefinition = new CosmosDBCollectionDefinition
//            {
//                Endpoint = this.cosmosDBEndpoint,
//                CollectionName = this.cosmosDBLeaseManagementCollection,
//                DbName = "durabletask",
//                SecretKey = this.cosmosDBAuthKey,

//            };

//            await Utils.CreateCollectionIfNotExists(cosmosDbCollectionDefinition);
//            this.stats.CosmosDBRequests.Increment();

//            for (int i = 0; i < eventHubInfo.PartitionCount; i++)
//            {
//                await CreatePartitionDocumentIfNotExist(i, eventHubInfo.TaskHubName);
//            } 

//            await this.CreateTaskHubInfoIfNotExistAsync(eventHubInfo);

//            return result;
//        }


//        public class CosmosDBLease : Lease
//        {
//            public string Id
//            {
//                get { return this.PartitionId; }
//                set { this.PartitionId = value; }
//            }

//            public string TaskHubName { get; set; }
//        }


//        private async Task CreatePartitionDocumentIfNotExist(int i, string taskHubName)
//        {
//            var lease = new CosmosDBLease()
//            {
//                PartitionId = string.Concat(taskHubName, "-", i.ToString()),
//                TaskHubName = taskHubName
//            };

//            await this.documentClient.CreateDocumentAsync(
//                UriFactory.CreateDocumentUri(this.cosmosDBName, this.cosmosDBLeaseManagementCollection, lease.Id),
//                lease);

//            this.stats.CosmosDBRequests.Increment();
//        }

//        public Task<IEnumerable<Lease>> ListLeasesAsync()
//        {
//            var blobLeases = new List<CosmosDBLease>();

//            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
//            var leases = this.documentClient.CreateDocumentQuery<CosmosDBLease>(
//                UriFactory.CreateDocumentCollectionUri(this.cosmosDBName, this.cosmosDBLeaseManagementCollection), queryOptions)
//                .Where(l => l.TaskHubName == this.taskHubName);

//            return Task.FromResult<IEnumerable<Lease>>(leases);
//        }

//        public async Task CreateLeaseIfNotExistAsync(string partition)
//        {
//            this.documentClient.ExecuteStoredProcedureAsync<StoredProcedureResponse<CosmosDBLease>>(
//                UriFactory.CreateStoredProcedureUri(cosmosDBName, cosmosDBLeaseManagementCollection, GetLeaseStoredProcedureName),
//                GetPartitionId(partition),
//                this.workerId,
                  
//                )
//            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(partition);
//            var lease = new CosmosDBLease()
//            {
//                PartitionId = partition,
//                Owner = this.workerId,
//                TaskHubName = this.taskHubName,
//                Epoch = 0
//            };
//             try
//            {
//                AnalyticsEventSource.Log.PartitionManagerInfo(
//                    this.cosmosDBLeaseManagementCollection,
//                    this.taskHubName,
//                    this.workerId,
//                    string.Format(
//                        CultureInfo.InvariantCulture,
//                        "CreateLeaseIfNotExistAsync - collectionName: {0}, taskHubName: {1}, partitionId: {2}",
//                        this.cosmosDBLeaseManagementCollection,
//                        this.taskHubName,
//                        partition));

//                await leaseBlob.UploadTextAsync(serializedLease, null, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null);
//            }
//            catch (StorageException se)
//            {
//                // eat any storage exception related to conflict
//                // this means the blob already exist
//                AnalyticsEventSource.Log.PartitionManagerInfo(
//                    this.storageAccountName,
//                    this.taskHubName,
//                    this.workerName,
//                    string.Format(
//                        CultureInfo.InvariantCulture,
//                        "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, consumerGroupName: {1}, partitionId: {2}, blobPrefix: {3}, exception: {4}.",
//                        this.leaseContainerName,
//                        this.consumerGroupName,
//                        partition,
//                        this.blobPrefix ?? string.Empty,
//                        se));
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }
//        }

//        private dynamic[] GetPartitionId(string partition)
//        {
//            throw new NotImplementedException();
//        }

//        public async Task<Lease> GetLeaseAsync(string paritionId)
//        {
//            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(paritionId);
//            if (await leaseBlob.ExistsAsync())
//            {
//                return await this.DownloadLeaseBlob(leaseBlob);
//            }

//            return null;
//        }

//        public async Task<bool> RenewAsync(Lease lease)
//        {
//            CloudBlockBlob leaseBlob = ((BlobLease)lease).Blob;
//            try
//            {
//                await leaseBlob.RenewLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token), options: this.renewRequestOptions, operationContext: null);
//            }
//            catch (StorageException storageException)
//            {
//                throw HandleStorageException(lease, storageException);
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }

//            return true;
//        }

//        public async Task<bool> AcquireAsync(Lease lease, string owner)
//        {
//            CloudBlockBlob leaseBlob = ((BlobLease)lease).Blob;
//            try
//            {
//                await leaseBlob.FetchAttributesAsync();
//                string newLeaseId = Guid.NewGuid().ToString("N");
//                if (leaseBlob.Properties.LeaseState == LeaseState.Leased)
//                {
//                    lease.Token = await leaseBlob.ChangeLeaseAsync(newLeaseId, accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token));
//                }
//                else
//                {
//                    lease.Token = await leaseBlob.AcquireLeaseAsync(this.leaseInterval, newLeaseId);
//                }

//                this.stats.StorageRequests.Increment();
//                lease.Owner = owner;
//                // Increment Epoch each time lease is acquired or stolen by new host
//                lease.Epoch += 1;
//                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
//                this.stats.StorageRequests.Increment();
//            }
//            catch (StorageException storageException)
//            {
//                this.stats.StorageRequests.Increment();
//                throw HandleStorageException(lease, storageException);
//            }

//            return true;
//        }

//        public async Task<bool> ReleaseAsync(Lease lease)
//        {
//            var blobLease = ((BlobLease)lease);
//            CloudBlockBlob leaseBlob = blobLease.Blob;
//            try
//            {
//                string leaseId = lease.Token;

//                BlobLease copy = new BlobLease(blobLease);
//                copy.Token = null;
//                copy.Owner = null;
//                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(copy), null, AccessCondition.GenerateLeaseCondition(leaseId), null, null);
//                this.stats.StorageRequests.Increment();
//                await leaseBlob.ReleaseLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId));
//                this.stats.StorageRequests.Increment();
//            }
//            catch (StorageException storageException)
//            {
//                this.stats.StorageRequests.Increment();
//                throw HandleStorageException(lease, storageException);
//            }

//            return true;
//        }

//        public async Task DeleteAsync(Lease lease)
//        {
//            CloudBlockBlob leaseBlob = ((BlobLease)lease).Blob;
//            try
//            {
//                await leaseBlob.DeleteIfExistsAsync();
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }
//        }

//        public async Task DeleteAllAsync()
//        {
//            try
//            {
//                await this.taskHubContainer.DeleteIfExistsAsync();
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }
//        }

//        public async Task<bool> UpdateAsync(Lease lease)
//        {
//            if (lease == null || string.IsNullOrWhiteSpace(lease.Token))
//            {
//                return false;
//            }

//            CloudBlockBlob leaseBlob = ((BlobLease)lease).Blob;
//            try
//            {
//                // First renew the lease to make sure checkpoint will go through
//                await leaseBlob.RenewLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token));
//            }
//            catch (StorageException storageException)
//            {
//                throw HandleStorageException(lease, storageException);
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }

//            try
//            {
//                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
//            }
//            catch (StorageException storageException)
//            {
//                throw HandleStorageException(lease, storageException, true);
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }

//            return true;
//        }

//        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
//        {
//            string serializedInfo = JsonConvert.SerializeObject(taskHubInfo);
//            try
//            {
//                await this.taskHubInfoBlob.UploadTextAsync(serializedInfo, null, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null);
//            }
//            catch (StorageException)
//            {
//                // eat any storage exception related to conflict
//                // this means the blob already exist
//            }
//            finally
//            {
//                this.stats.StorageRequests.Increment();
//            }
//        }

//        internal async Task<TaskHubInfo> GetOrCreateTaskHubInfoAsync(TaskHubInfo createdTaskHubInfo)
//        {
//            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
//            if (currentTaskHubInfo != null)
//            {
//                return currentTaskHubInfo;
//            }

//            await this.CreateTaskHubInfoIfNotExistAsync(createdTaskHubInfo);
//            return createdTaskHubInfo;
//        }

//        internal async Task<bool> IsStaleLeaseStore(TaskHubInfo taskHubInfo)
//        {
//            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
//            if (currentTaskHubInfo != null)
//            {
//                if (!currentTaskHubInfo.TaskHubName.Equals(taskHubInfo.TaskHubName, StringComparison.OrdinalIgnoreCase)
//                    || !currentTaskHubInfo.CreatedAt.Equals(taskHubInfo.CreatedAt)
//                    || !currentTaskHubInfo.PartitionCount.Equals(taskHubInfo.PartitionCount))
//                {
//                    return true;
//                }
//            }

//            return false;
//        }

//        void Initialize()
//        {
//            this.documentClient = new DocumentClient(new Uri(this.cosmosDBEndpoint), this.cosmosDBAuthKey);

           
          
//        }

//        async Task<TaskHubInfo> GetTaskHubInfoAsync()
//        {
//            if (await this.taskHubInfoBlob.ExistsAsync())
//            {
//                await taskHubInfoBlob.FetchAttributesAsync();
//                this.stats.StorageRequests.Increment();
//                string serializedEventHubInfo = await this.taskHubInfoBlob.DownloadTextAsync();
//                this.stats.StorageRequests.Increment();
//                return JsonConvert.DeserializeObject<TaskHubInfo>(serializedEventHubInfo);
//            }

//            this.stats.StorageRequests.Increment();
//            return null;
//        }

//        async Task<BlobLease> DownloadLeaseBlob(CloudBlockBlob blob)
//        {
//            string serializedLease = await blob.DownloadTextAsync();
//            this.stats.StorageRequests.Increment();
//            BlobLease deserializedLease = JsonConvert.DeserializeObject<BlobLease>(serializedLease);
//            deserializedLease.Blob = blob;

//            // Workaround: for some reason storage client reports incorrect blob properties after downloading the blob
//            await blob.FetchAttributesAsync();
//            this.stats.StorageRequests.Increment();
//            return deserializedLease;
//        }

//        static Exception HandleStorageException(Lease lease, StorageException storageException, bool ignoreLeaseLost = false)
//        {
//            if (storageException.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict
//                || storageException.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
//            {
//                // Don't throw LeaseLostException if caller chooses to ignore it.
//                StorageExtendedErrorInformation extendedErrorInfo = storageException.RequestInformation.ExtendedErrorInformation;
//                if (!ignoreLeaseLost || extendedErrorInfo == null || extendedErrorInfo.ErrorCode != BlobErrorCodeStrings.LeaseLost)
//                {
//                    return new LeaseLostException(lease, storageException);
//                }
//            }

//            return storageException;
//        }
//    }
//}