﻿using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace DurableTask.CosmosDB
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
        /// Collection throughtput
        /// Default is 400
        /// </summary>
        public int Throughput { get; set; } = 400;

        /// <summary>
        /// Partition key paths for the collection
        /// </summary>
        public Collection<string> PartitionKeyPaths { get; set; }

        /// <summary>
        /// Included index paths
        /// </summary>
        public IList<IncludedPath> IndexIncludedPaths { get; set; }

        /// <summary>
        /// Excluded index paths
        /// </summary>
        public IList<ExcludedPath> IndexExcludedPaths { get; set; }
    }
}
