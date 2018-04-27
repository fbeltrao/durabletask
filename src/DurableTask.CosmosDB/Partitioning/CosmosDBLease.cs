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

using DurableTask.AzureStorage.Partitioning;
using Newtonsoft.Json;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// CosmosDB lease
    /// </summary>
    class CosmosDBLease : Lease
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBLease()
        {

        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="source"></param>
        public CosmosDBLease(CosmosDBLease source) : base(source)
        {
            this.TaskHubName = source.TaskHubName;
        }

        /// <summary>
        /// Document id
        /// </summary>
        [JsonProperty("id")]
        public string Id
        {
            get { return this.PartitionId; }
            set { this.PartitionId = value; }
        }

        /// <summary>
        /// Task hub name
        /// </summary>
        public string TaskHubName { get; set; }
    }
}