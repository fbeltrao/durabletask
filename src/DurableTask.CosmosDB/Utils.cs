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

namespace DurableTask.AzureStorage
{
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.CosmosDB;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.WindowsAzure.Storage;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    static class Utils
    {
        public static readonly Task CompletedTask = Task.FromResult(0);

        public static async Task ParallelForEachAsync<TSource>(
            this IEnumerable<TSource> enumerable,
            Func<TSource, Task> createTask)
        {
            var tasks = new List<Task>();
            foreach (TSource entry in enumerable)
            {
                tasks.Add(createTask(entry));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        internal const int DefaultPartitionCount = 4;

        /// <summary>
        /// Checks whether collections exists. Creates new collection if collection does not exist 
        /// WARNING: CreateCollectionIfNotExistsAsync will create a new 
        /// with reserved throughput which has pricing implications. For details
        /// visit: https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
        /// </summary>
        /// <returns>A Task to allow asynchronous execution</returns>
        internal static async Task CreateCollectionIfNotExists(CosmosDBCollectionDefinition collectionDefinition)
        { 
            // connecting client 
            using (var client = new DocumentClient(new Uri(collectionDefinition.Endpoint), collectionDefinition.SecretKey))
            {
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = collectionDefinition.DbName });

                // create collection if it does not exist 
                // WARNING: CreateDocumentCollectionIfNotExistsAsync will create a new 
                // with reserved throughput which has pricing implications. For details
                // visit: https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(collectionDefinition.DbName),
                    new DocumentCollection { Id = collectionDefinition.CollectionName },
                    new RequestOptions { OfferThroughput = collectionDefinition.Throughput });
            }
        }


        static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        internal static long ToUnixTime(DateTime date)
        {
            return Convert.ToInt64((date - epoch).TotalSeconds);
        }
    }


}
