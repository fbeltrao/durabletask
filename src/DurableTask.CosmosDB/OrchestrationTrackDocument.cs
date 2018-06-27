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
    using DurableTask.Core.History;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Collections.Generic;

    /// <summary>
    /// Represent a CosmosDb Track document. This is where the history for the durable function is.
    /// </summary>
    public class OrchestrationTrackDocument : Document
    {
        /// <summary>
        /// Default constructor for <see cref="OrchestrationTrackDocument"/>
        /// </summary>
        public OrchestrationTrackDocument()
        {
            History = new Dictionary<string, List<JObject>>();
        }

        /// <summary>
        /// History execution
        /// </summary>
        [JsonProperty("history")]
        public IDictionary<string, List<JObject>> History { get; set; }



        /// <summary>
        /// This is the orchestrator instance id
        /// </summary>
        [JsonProperty("instanceId")]
        public string InstanceId { get; set; }
    }
}
