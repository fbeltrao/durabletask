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


namespace DurableTask.CosmosDB.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using System.Linq;
    using Microsoft.Azure.Documents.Linq;

    class CosmosDbTrackingStore : TrackingStoreBase
    {
        private readonly DocumentClient documentClient;
        private string instancesCollectionName;
        private string historyCollectionName;
        private string DatabaseName;

        public CosmosDbTrackingStore(string endpoint, string key, string instanceCollection, string historyCollection, string databaseName)
        {
            this.documentClient = new DocumentClient(new Uri(endpoint), key);
            this.instancesCollectionName = instanceCollection;
            this.historyCollectionName = historyCollection;
            this.DatabaseName = databaseName;
        }

        public override async Task CreateAsync()
        {
            var instanceCollection = new DocumentCollection()
            {
                Id = this.instancesCollectionName,
            };

            instanceCollection.PartitionKey.Paths.Add("/instanceId");

            await documentClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(DatabaseName),
                instanceCollection,
                new RequestOptions { OfferThroughput = 10000 });


            var historyCollection = new DocumentCollection()
            {
                Id = this.historyCollectionName,
            };

            historyCollection.PartitionKey.Paths.Add("/instanceId");

            await documentClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(DatabaseName),
                historyCollection,
                new RequestOptions { OfferThroughput = 10000 });

        }

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public override Task<bool> ExistsAsync()
        {
            throw new NotImplementedException();
        }

        public override Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions)
        {
            return (await GetDocumentStateAsync(instanceId)).Executions.Values.ToList();
        }

        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            return (await GetDocumentStateAsync(instanceId)).Executions[executionId];
        }

        private async Task<OrchestrationStateDocument> GetDocumentStateAsync(string instanceId)
        {
            OrchestrationStateDocument result = null;

            var collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.instancesCollectionName);
            var query = this.documentClient.CreateDocumentQuery<OrchestrationStateDocument>(collectionUri, new FeedOptions()
            {
                PartitionKey = new PartitionKey(instanceId)

            })
                .Where(p => p.InstanceId == instanceId).AsDocumentQuery();

            var list = await query.ExecuteNextAsync<OrchestrationStateDocument>();

            if (list != null && list.Count > 0)
            {
                result = list.FirstOrDefault();
            }

            return result;
        }

        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public override async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            OrchestrationTrackDocument document = await GetHistoryDocument(executionStartedEvent.OrchestrationInstance.InstanceId);
            if (document == null)
            {
                document = new OrchestrationTrackDocument();
                document.InstanceId = executionStartedEvent.OrchestrationInstance.InstanceId;
            }

            string executionId = executionStartedEvent.OrchestrationInstance.ExecutionId;
            if (!document.History.ContainsKey(executionId))
            {
                document.History.Add(executionId, new List<HistoryEvent>());
            }

            document.History[executionId].Add(executionStartedEvent);
            document.SetPropertyValue("history", document.History);

            await SaveHistoryDocument(document);
        }

        private Task<ResourceResponse<Document>> SaveHistoryDocument(OrchestrationTrackDocument value)
        {
            var documentUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.historyCollectionName);
            return documentClient.UpsertDocumentAsync(documentUri, value, new RequestOptions()
            {
                PartitionKey = new PartitionKey(value.InstanceId)
            });
        }

        private async Task<OrchestrationTrackDocument> GetHistoryDocument(string instanceId)
        {
            OrchestrationTrackDocument result = null;

            if (!string.IsNullOrEmpty(instanceId))
            {
                var collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.historyCollectionName);
                var query = this.documentClient.CreateDocumentQuery<OrchestrationStateDocument>(collectionUri, new FeedOptions()
                {
                    PartitionKey = new PartitionKey(instanceId)

                })
                    .Where(p => p.InstanceId == instanceId).AsDocumentQuery();

                var list = await query.ExecuteNextAsync<OrchestrationTrackDocument>();

                if (list != null && list.Count > 0)
                {
                    result = list.FirstOrDefault();
                }
            }

            return result;
        }

        public override Task StartAsync()
        {
            throw new NotImplementedException();
        }

        public override Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }
    }
}
