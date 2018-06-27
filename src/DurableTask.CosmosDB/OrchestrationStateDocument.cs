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

namespace DurableTask.CosmosDB
{
    using DurableTask.Core;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using System.Collections.Generic;

    /// <summary>
    /// Orchestration State document
    /// </summary>
    public class OrchestrationStateDocument : Document
    {
        /// <summary>
        /// Executions
        /// </summary>
        [JsonProperty("executions")]
        public IDictionary<string, OrchestrationState> Executions { get; set; }

        /// <summary>
        /// This is the orchestrator instance id
        /// </summary>
        [JsonProperty("instanceId")]
        public string InstanceId { get; set; }

        /// <summary>
        /// This flag indicates that the executions are stored as attachment
        /// </summary>
        [JsonProperty("executionsInAttachment")]
        public bool ExecutionsInAttachment { get;  set; }
    }
}
