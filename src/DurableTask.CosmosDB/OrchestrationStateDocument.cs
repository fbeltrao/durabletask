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
