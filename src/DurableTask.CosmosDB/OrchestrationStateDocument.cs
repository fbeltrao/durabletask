using DurableTask.Core;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB
{

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
    }
}
