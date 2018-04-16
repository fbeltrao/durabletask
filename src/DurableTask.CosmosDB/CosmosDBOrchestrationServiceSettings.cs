using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB
{
    /// <summary>
    /// Settings for <see cref="CosmosDBOrchestrationService"/>
    /// </summary>
    public class CosmosDBOrchestrationServiceSettings
    {
        /// <summary>
        /// CosmosDB endpoint
        /// </summary>
        public string CosmosDBEndpoint { get; set; }

        /// <summary>
        /// CosmosDB authorization key
        /// </summary>
        public string CosmosDBAuthKey { get; set; }

        /// <summary>
        /// Hub name
        /// </summary>
        public string TaskHubName { get; set; }

        
    }
}
