﻿//  ----------------------------------------------------------------------------------
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
    using System.Diagnostics;
    using System.Data.SqlClient;

    class SqlTrackingStore : TrackingStoreBase
    {
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;
        private readonly string connectionString;        
        public SqlTrackingStore(string connectionString)
        {
            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
            this.connectionString = connectionString;
        }

        public override Task CreateAsync()
        {
            // TODO: create tables
            return Task.FromResult(0);
        }

        public override Task DeleteAsync()
        {
            //try
            //{
            //    await this.documentClient.DeleteDocumentCollectionAsync(this.historyCollectionName);
            //    await this.documentClient.DeleteDocumentCollectionAsync(this.instancesCollectionName);
            //}
            //catch (Exception)
            //{

            //}

            return Task.FromResult(0);
        }

        public override Task<bool> ExistsAsync()
        {
            // TODO: check if tables exist
            return Task.FromResult(true);
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
            else if (documentHistory != null && string.IsNullOrEmpty(expectedExecutionId))
            {
                foreach (var historyItem in documentHistory.History.Last().Value)
                {
                    result.Add((HistoryEvent)converter.ConvertFromTableEntity(historyItem, GetTypeForJsonObject));
                }
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
            return new[] { await this.GetStateAsync(instanceId, executionId: null) };
        }

        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            OrchestrationState result = null;

            OrchestrationStateDocument document = await GetDocumentStateAsync(instanceId);
            if (document != null && !string.IsNullOrEmpty(executionId) && document.Executions.ContainsKey(executionId))
            {
                result = document.Executions[executionId];
            }
            else if (document != null && string.IsNullOrEmpty(executionId))
            {
                result = document.Executions.Values.LastOrDefault();
            }

            Trace.WriteLine($"ReadState {result.OrchestrationStatus} | {result.Status}");
            return result;
        }

        SqlConnection GetConnection() => new SqlConnection(connectionString);

        private async Task<OrchestrationStateDocument> GetDocumentStateAsync(string instanceId)
        {
            OrchestrationStateDocument result = null;

            object resultColumnValue = null;
            using (var conn = GetConnection())
            {
                var cmd = new SqlCommand("up_instance_Get", conn)
                {
                    CommandType = System.Data.CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@instanceId", instanceId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                await conn.OpenAsync();


                resultColumnValue = await cmd.ExecuteScalarAsync();                
            }

            if (resultColumnValue != null && !Convert.IsDBNull(resultColumnValue))
            {
                result = JsonConvert.DeserializeObject<OrchestrationStateDocument>((string)resultColumnValue);
            }

            return result;
        }

        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public override async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            OrchestrationStateDocument value = await GetDocumentStateAsync(executionStartedEvent.OrchestrationInstance.InstanceId);
            if (value == null)
            {
                value = new OrchestrationStateDocument()
                {
                    InstanceId = executionStartedEvent.OrchestrationInstance.InstanceId,
                };

                value.Executions = new Dictionary<string, OrchestrationState>();
            }
            
            value.Executions[executionStartedEvent.OrchestrationInstance.ExecutionId] = new OrchestrationState()
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = executionStartedEvent.OrchestrationInstance.InstanceId,
                    ExecutionId = executionStartedEvent.OrchestrationInstance.ExecutionId,
                },
                Input = executionStartedEvent.Input,
                CreatedTime = executionStartedEvent.Timestamp,
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                LastUpdatedTime = executionStartedEvent.Timestamp,
                Status = OrchestrationStatus.Pending.ToString(),
                OrchestrationStatus = OrchestrationStatus.Pending
            };

            value.SetPropertyValue("executions", value.Executions);
            await UpsertOrchestrationState(value);
        }

        private async Task UpsertOrchestrationState(OrchestrationStateDocument value)
        {
            using (var conn = GetConnection())
            {
                var cmd = new SqlCommand("up_instance_Upsert", conn)
                {
                    CommandType = System.Data.CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@instanceId", value.InstanceId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@data", JsonConvert.SerializeObject(value)) { SqlDbType = System.Data.SqlDbType.NVarChar, Size = -1 });
                await conn.OpenAsync();

                await cmd.ExecuteNonQueryAsync();
            }
        }

        private async Task SaveHistoryDocument(OrchestrationTrackDocument value)
        {

            using (var conn = GetConnection())
            {
                var cmd = new SqlCommand("up_history_Upsert", conn)
                {
                    CommandType = System.Data.CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@instanceId", value.InstanceId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@data", JsonConvert.SerializeObject(value)) { SqlDbType = System.Data.SqlDbType.NVarChar, Size = -1 });
                await conn.OpenAsync();

                await cmd.ExecuteNonQueryAsync();
            }
        }

        private async Task<OrchestrationTrackDocument> GetHistoryDocument(string instanceId)
        {
            OrchestrationTrackDocument result = null;

            object resultColumnValue = null;
            using (var conn = GetConnection())
            {
                var cmd = new SqlCommand("up_history_Get", conn)
                {
                    CommandType = System.Data.CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@instanceId", instanceId) { SqlDbType = System.Data.SqlDbType.VarChar, Size = 100 });
                await conn.OpenAsync();


                resultColumnValue = await cmd.ExecuteScalarAsync();
            }

            if (resultColumnValue != null && !Convert.IsDBNull(resultColumnValue))
            {
                result = JsonConvert.DeserializeObject<OrchestrationTrackDocument>((string)resultColumnValue);
            }

            return result;
        }

        public override Task StartAsync()
        {
            return Task.FromResult(0);
        }

        public override async Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            OrchestrationTrackDocument document = await GetHistoryDocument(instanceId);
            if (document == null)
            {
                document = new OrchestrationTrackDocument();
                document.InstanceId = instanceId;
            }

            if (!document.History.ContainsKey(executionId))
            {
                document.History.Add(executionId, new List<JObject>());
            }

            OrchestrationStateDocument value = await GetDocumentStateAsync(instanceId);
            if (value == null)
            {
                value = new OrchestrationStateDocument()
                {
                    InstanceId = instanceId,
                };

                value.Executions = new Dictionary<string, OrchestrationState>();
            }
            EventType? orchestratorEventType = null;
            OrchestrationState state;
            if (!string.IsNullOrEmpty(executionId) && value.Executions.ContainsKey(executionId))
            {
                state = value.Executions[executionId];
            }
            else
            {
                state = new OrchestrationState();
            }

            foreach (var historyEvent in runtimeState.NewEvents)
            {
                Trace.WriteLine($"EventType: {historyEvent.EventType}");
                document.History[executionId].Add(new JsonEntityConverter().ConvertToTableEntity(historyEvent));

                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        state.Name = executionStartedEvent.Name;
                        state.Version = executionStartedEvent.Version;
                        state.CreatedTime = executionStartedEvent.Timestamp;
                        state.Status = OrchestrationStatus.Running.ToString();
                        state.OrchestrationStatus = OrchestrationStatus.Running;
                        state.Input = executionStartedEvent.Input;
                        state.LastUpdatedTime = historyEvent.Timestamp;
                        break;
                    case EventType.ExecutionCompleted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        state.Status = executionCompleted.OrchestrationStatus.ToString();
                        state.OrchestrationStatus = executionCompleted.OrchestrationStatus;
                        state.Output = executionCompleted.Result;
                        state.LastUpdatedTime = historyEvent.Timestamp;
                        break;
                    case EventType.ExecutionTerminated:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        // TODO: Luis I think the property to be set is result and output
                        // look here: durabletask\src\DurableTask.AzureStorage\Tracking\AzureTableTrackingStore.cs line 418
                        //state.Input = executionTerminatedEvent.Input;
                        state.Output = executionTerminatedEvent.Input;
                        state.Status = OrchestrationStatus.Terminated.ToString();
                        state.OrchestrationStatus = OrchestrationStatus.Terminated;
                        state.LastUpdatedTime = historyEvent.Timestamp;
                        break;
                    case EventType.ContinueAsNew:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        state.Output = executionCompletedEvent.Result;
                        state.Status = OrchestrationStatus.ContinuedAsNew.ToString();
                        state.OrchestrationStatus = OrchestrationStatus.ContinuedAsNew;
                        state.LastUpdatedTime = historyEvent.Timestamp;
                        break;
                }
            }

            document.SetPropertyValue("history", document.History);
            await SaveHistoryDocument(document);

            if(state.OrchestrationInstance == null)
            {
                state.OrchestrationInstance = new OrchestrationInstance()
                {
                    ExecutionId = executionId,
                    InstanceId = instanceId
                };
                
            }

            if (!value.Executions.ContainsKey(executionId))
            {
                value.Executions.Add(executionId, state);
            }
            else
            {
                value.Executions[executionId] = state;
            }

            value.SetPropertyValue("executions", value.Executions);
            await UpsertOrchestrationState(value);            
        }        
    }
}