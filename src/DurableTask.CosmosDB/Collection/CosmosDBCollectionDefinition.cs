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

namespace DurableTask.CosmosDB.Collection
{
    /// <summary>
    /// Cosmos DB collection definition
    /// </summary>
    public class CosmosDBCollectionDefinition
    {
        /// <summary>
        /// Endpoint (example: https://localhost:8081 for local CosmosDB)
        /// </summary>
        public string Endpoint { get; set; }

        /// <summary>
        /// CosmosDB key
        /// </summary>
        public string SecretKey { get; set; }

        /// <summary>
        /// Database name
        /// </summary>
        public string DbName { get; set; }

        /// <summary>
        /// Collection name
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Collection throughput
        /// Default is 400
        /// </summary>
        public int Throughput { get; set; } = 400;
    }
}
