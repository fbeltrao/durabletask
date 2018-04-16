using DurableTask.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB
{

    /// <summary>
    /// Orchestration State document
    /// </summary>
    public class OrchestrationStateDocument
    {
        /// <summary>
        /// Instance ID
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; }

        /// <summary>
        /// Executions
        /// </summary>
        [JsonProperty("executions")]
        public IDictionary<string, OrchestrationState> Executions { get; set; }
    }
}
