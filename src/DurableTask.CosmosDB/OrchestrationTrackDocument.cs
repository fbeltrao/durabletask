﻿namespace DurableTask.CosmosDB
{
    using DurableTask.Core.History;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
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
            History = new Dictionary<string, List<HistoryEvent>>();
        }

        /// <summary>
        /// History execution
        /// </summary>
        [JsonProperty("history")]
        public IDictionary<string, List<HistoryEvent>> History { get; set; }

        /// <summary>
        /// This is the orchestrator instance id
        /// </summary>
        [JsonProperty("instanceId")]
        public string InstanceId { get; set; }
    }
}
