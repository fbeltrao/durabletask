using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB
{
    /// <summary>
    /// Cosmos db queue
    /// </summary>
    public class CosmosDBQueue : IDisposable
    {
        private CosmosDBQueueSettings settings;
        DocumentClient queueCollectionClient;
        bool initialized = false;



        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueue()
        {
        }



       
        /// <summary>
        /// Initializes the queue
        /// </summary>
        /// <param name="settings"></param>
        public async Task Initialize(CosmosDBQueueSettings settings)
        {
            if (this.initialized)
                throw new InvalidOperationException("Initialization already occured");

            this.settings = settings;
            await Utils.CreateCollectionIfNotExists(this.settings.QueueCollectionDefinition);

            this.queueCollectionClient = new DocumentClient(
                new Uri(this.settings.QueueCollectionDefinition.Endpoint), 
                this.settings.QueueCollectionDefinition.SecretKey,
                new JsonSerializerSettings()
                {
                    TypeNameHandling = TypeNameHandling.All
                });

            this.initialized = true;            
        }


        /// <summary>
        /// Queue item returning queue item identifier
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>t
        public async Task<string> Queue<T>(T payload) where T:class => await Queue(null, payload);

        /// <summary>
        /// Queue item returning queue item identifier
        /// </summary>
        /// <param name="id"></param>
        /// <param name="payload"></param>
        /// <returns></returns>
        public async Task<string> Queue<T>(string id, T payload) where T: class
        {
            if (!this.initialized)            
                throw new InvalidOperationException("Must call Initialize() first");            

            var queueItem = new CosmosDBQueueItem<T>()
            {
                id = id,
                data = payload
            };

            var uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);

            var res = await this.queueCollectionClient.CreateDocumentAsync(uri, queueItem);


            return res.Resource.Id;
        }


        /// <summary>
        /// Dequeues next time
        /// </summary>
        /// <returns></returns>
        public async Task<CosmosDBQueueItem<T>> Dequeue<T>() where T:class
        {
            try
            {
                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName, "peekItem");
                var response = await this.queueCollectionClient.ExecuteStoredProcedureAsync<string>(storedProcedureUri);
                if (!string.IsNullOrEmpty(response.Response))
                {
                    var result =  JsonConvert.DeserializeObject<CosmosDBQueueItem<T>>(response.Response,
                        new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.All
                        });

                    return result;
                }

                return null;
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }

        internal async Task CompleteAsync(string id)
        {
            var documentUri = UriFactory.CreateDocumentUri(
                settings.QueueCollectionDefinition.DbName,
                settings.QueueCollectionDefinition.CollectionName,
                id);

            await this.queueCollectionClient.DeleteDocumentAsync(documentUri);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.queueCollectionClient?.Dispose();
                    this.queueCollectionClient = null;
                }

                disposedValue = true;
            }
        }
        

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {           
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
