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
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using DurableTask.AzureStorage;
    using System.Reflection;
    using System.Runtime.Serialization;

    class CosmosDbTrackingStore : TrackingStoreBase
    {
        private readonly DocumentClient documentClient;
        private string instancesCollectionName;
        private string historyCollectionName;
        private string DatabaseName;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;

        public Microsoft.WindowsAzure.Storage.Table.CloudTable HistoryTable { get; internal set; }

        public CosmosDbTrackingStore(string endpoint, string key, string instanceCollection, string historyCollection, string databaseName)
        {
            this.documentClient = new DocumentClient(new Uri(endpoint), key, new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.All,
            });

            this.instancesCollectionName = instanceCollection;
            this.historyCollectionName = historyCollection;
            this.DatabaseName = databaseName;

            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
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

        public override async Task DeleteAsync()
        {
            try
            {
                await this.documentClient.DeleteDocumentCollectionAsync(this.historyCollectionName);
                await this.documentClient.DeleteDocumentCollectionAsync(this.instancesCollectionName);
            }
            catch (Exception)
            {

            }


        }

        public override async Task<bool> ExistsAsync()
        {
            bool result = false;
            var instanceCollection = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.instancesCollectionName);
            var historyCollection = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.historyCollectionName);
            var instanceCollectionValue = await this.documentClient.ReadDocumentCollectionAsync(instanceCollection);
            var historyCollectionValue = await this.documentClient.ReadDocumentCollectionAsync(historyCollection);
            result = instanceCollectionValue.Resource != null && historyCollectionValue.Resource != null;
            return result;
        }

        public override async Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default)
        {
            List<HistoryEvent> result = new List<HistoryEvent>();
            OrchestrationTrackDocument documentHistory = await GetHistoryDocument(instanceId);
            JsonEntityConverter converter = new JsonEntityConverter();
            List<JObject> list = null;
            if (documentHistory != null && !string.IsNullOrEmpty(expectedExecutionId) && documentHistory.History.ContainsKey(expectedExecutionId))
            {
                list = documentHistory.History[expectedExecutionId];
            }
            else if (documentHistory != null)
            {
                list = documentHistory.History.Values.FirstOrDefault();
            }

            if (list != null)
            {
                foreach (var item in list)
                {
                    result.Add((HistoryEvent)converter.ConvertFromTableEntity(item, GetTypeForJsonObject));
                }
            }

            return result;
        }

        Type GetTypeForJsonObject(JObject jsonEntity)
        {
            EventType eventType;
            if (jsonEntity["EventType"] != null)
            {
                eventType = (EventType)Enum.Parse(typeof(EventType), jsonEntity["EventType"].Value<string>());
            }
            else
            {
                throw new ArgumentException($"{jsonEntity["EventType"]} is not a valid EventType value.");
            }

            return this.eventTypeMap[eventType];
        }

        public override async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions)
        {
            List<OrchestrationState> result = new List<OrchestrationState>();

            OrchestrationStateDocument document = await GetDocumentStateAsync(instanceId);
            if (document != null)
            {
                result = document.Executions.Values.ToList();
            }

            return result;

        }

        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            OrchestrationState result = null;

            OrchestrationStateDocument document = await GetDocumentStateAsync(instanceId);
            if (document != null && !string.IsNullOrEmpty(executionId) && document.Executions.ContainsKey(executionId))
            {
                result = document.Executions[executionId];
            }
            else if (document != null && string.IsNullOrEmpty(executionId))
            {
                result = document.Executions.Values.FirstOrDefault();
            }

            return result;
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
                document.History.Add(executionId, new List<JObject>());
            }

            document.History[executionId].Add(new JsonEntityConverter().ConvertToTableEntity(executionStartedEvent));
            document.SetPropertyValue("history", document.History);

            await SaveHistoryDocument(document);
            await SaveExecutionInstance(executionStartedEvent);
        }

        private async Task SaveExecutionInstance(ExecutionStartedEvent executionStartedEvent)
        {
            var stateDocument = await GetDocumentStateAsync(executionStartedEvent.OrchestrationInstance.InstanceId);

            OrchestrationState newState = new OrchestrationState()
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = executionStartedEvent.OrchestrationInstance.InstanceId,
                    ExecutionId = executionStartedEvent.OrchestrationInstance.ExecutionId,
                },
                CreatedTime = DateTime.UtcNow,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Version = executionStartedEvent.Version,
                Name = executionStartedEvent.Name,
                Input = executionStartedEvent.Input,
            };


            if (stateDocument == null)
            {
                stateDocument = new OrchestrationStateDocument()
                {
                    InstanceId = executionStartedEvent.OrchestrationInstance.InstanceId
                };
                stateDocument.Executions = new Dictionary<string, OrchestrationState>();
            }

            newState.LastUpdatedTime = DateTime.UtcNow;
            stateDocument.Executions[newState.OrchestrationInstance.ExecutionId] = newState;
            ResourceResponse<Document> result = await UpsertOrchestrationState(stateDocument);
        }

        private async Task<ResourceResponse<Document>> UpsertOrchestrationState(OrchestrationStateDocument value)
        {
            var documentUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.instancesCollectionName);
            return await documentClient.UpsertDocumentAsync(documentUri, value, new RequestOptions()
            {
                PartitionKey = new PartitionKey(value.InstanceId)
            });
        }

        private async Task<ResourceResponse<Document>> SaveHistoryDocument(OrchestrationTrackDocument value)
        {
            var documentUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, this.historyCollectionName);
            return await documentClient.UpsertDocumentAsync(documentUri, value, new RequestOptions()
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
            return Task.FromResult(0);
        }

        public override async Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            OrchestrationStateDocument value = await GetDocumentStateAsync(instanceId);
            if (value == null)
            {
                value = new OrchestrationStateDocument()
                {
                    InstanceId = instanceId,
                };

                value.Executions = new Dictionary<string, OrchestrationState>();
            }


            value.Executions[runtimeState.OrchestrationInstance.ExecutionId] = ConvertRochestrationRuntimeState(runtimeState);
            value.SetPropertyValue("executions", value.Executions);
            await UpsertOrchestrationState(value);
        }

        private OrchestrationState ConvertRochestrationRuntimeState(OrchestrationRuntimeState value)
        {
            return new OrchestrationState()
            {
                CompletedTime = value.CompletedTime,
                CompressedSize = value.CompressedSize,
                CreatedTime = value.CreatedTime,
                Input = value.Input,
                LastUpdatedTime = DateTime.UtcNow,
                Name = value.Name,
                OrchestrationInstance = value.OrchestrationInstance,
                OrchestrationStatus = value.OrchestrationStatus,
                Output = value.Output,
                ParentInstance = value.ParentInstance,
                Size = value.Size,
                Status = value.Status,
                Tags = value.Tags,
                Version = value.Version
            };
        }
    }
}
