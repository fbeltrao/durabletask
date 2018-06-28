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
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.CosmosDB.Collection;

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

        internal static DateTime FromUnixTime(long value)
        {
            return epoch.AddSeconds(value);
        }

        internal static string GetControlQueueId(string taskHubName, int partitionIndex)
        {
            return $"{taskHubName.ToLowerInvariant()}hub-control-{partitionIndex:00}";
        }

        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        /// <typeparam name="V"></typeparam>
        /// <param name="function"></param>
        /// <returns></returns>
        internal static async Task<V> ExecuteWithRetries<V>(Func<Task<V>> function)
        {
            TimeSpan sleepTime = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await function();
                }
                catch (DocumentClientException de)
                {
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    sleepTime = de.RetryAfter;
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException de = (DocumentClientException)ae.InnerException;
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }

        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        /// <typeparam name="V"></typeparam>
        /// <param name="function"></param>
        /// <param name="delayIfNotFound"></param>
        /// <param name="maxNotFoundAttempts"></param>
        /// <returns></returns>
        internal static async Task<V> ExecuteWithRetries<V>(Func<Task<V>> function, TimeSpan delayIfNotFound, int maxNotFoundAttempts = 1)
        {
            var notFoundResultCount = 0;
            TimeSpan sleepTime = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await function();
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        notFoundResultCount++;
                        if (notFoundResultCount > maxNotFoundAttempts)
                            throw;

                        sleepTime = delayIfNotFound;
                    }
                    else if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    else
                    {
                        sleepTime = de.RetryAfter;
                    }
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException de = (DocumentClientException)ae.InnerException;
                    if ((int)de.StatusCode != 429)
                    {
                        throw;
                    }
                    sleepTime = de.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }
        }

        internal static async Task<ResourceResponse<Attachment>> SaveAttachment(DocumentClient client, Uri documentUri, object attachment, string attachmentId, RequestOptions requestOptions = null)
        {
            var attachmentJson = JsonConvert.SerializeObject(attachment, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All });

            const int maxAttempts = 3;
            var attempt = 0;
            while (true)
            {
                try
                {
                    return await InternalSaveAttachmentAsync(client, documentUri, attachmentJson, attachmentId, requestOptions);
                }
                catch (Exception)
                {
                    attempt++;

                    if (attempt >= maxAttempts)
                        throw;

                    await Task.Delay(100);
                }
            }   
        }

        private static async Task<ResourceResponse<Attachment>> InternalSaveAttachmentAsync(DocumentClient client, Uri documentUri, string attachmentJson, string attachmentId, RequestOptions requestOptions)
        {

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(attachmentJson)))
            {
                var mediaOptions = new MediaOptions()
                {
                    ContentType = "text/plain",
                    Slug = attachmentId
                };

                return await client.UpsertAttachmentAsync(
                        documentUri,
                        stream,
                        mediaOptions,
                        requestOptions);
                
            }
        }

        internal static async Task<T> LoadAttachment<T>(DocumentClient client, Uri attachmentUri, RequestOptions requestOptions = null) where T: class
        {
            try
            {
                var mediaResponse = await Utils.ExecuteWithRetries(async () => {
                        var maxAttempts = 3;
                        var delayTime = 500;
#if DEBUG
                        if (Debugger.IsAttached)
                        {
                            delayTime = 1000;
                            maxAttempts = 5;
                        }
#endif

                    var attempt = 0;
                        ResourceResponse<Attachment> attachment = null;

                        while (true)
                        {
                            attachment = await client.ReadAttachmentAsync(attachmentUri, requestOptions);
                            if (attachment.Resource.MediaLink == "/media/placeholder")
                            {
                                attempt++;
                                if (attempt == maxAttempts)
                                    break;

                                await Task.Delay(delayTime * attempt);
                            }
                            else
                            {
                                break;
                            }
                        }

                        return await client.ReadMediaAsync(attachment.Resource.MediaLink);
                    }, 
                    TimeSpan.FromMilliseconds(500),
                    4);
                var payload = await new StreamReader(mediaResponse.Media, Encoding.UTF8).ReadToEndAsync();
                return (T)JsonConvert.DeserializeObject(payload, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.Objects });
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }
    }

}
