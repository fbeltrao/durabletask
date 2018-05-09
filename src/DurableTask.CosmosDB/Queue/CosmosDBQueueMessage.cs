using DurableTask.AzureStorage;
using DurableTask.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Cosmos DB queue message
    /// </summary>
    class CosmosDBQueueMessage : IQueueMessage
    {
        [JsonProperty("insertionTime")]
        public DateTimeOffset? InsertionTime { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("dequeueCount")]
        public int DequeueCount { get; set; }

        [JsonProperty("nextVisibleTime")]
        public DateTimeOffset? NextVisibleTime { get; set; }

        public long TimeToLive { get; set; }

        public object Data { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public QueueItemStatus status { get; set; }

        public long queuedTime { get; set; }

        public long processStartTime { get; set; }

        public long completedTime { get; set; }

        public string currentWorker { get; set; }


        public long workerExpires { get; set; }


        [JsonIgnore]
        public string etag { get; set; }

        public int errors { get; set; }

        public string queueName { get;  set; }

        public string AsString()
        {
            return JsonConvert.SerializeObject(this);
        }


        public void SetAsComplete()
        {
            this.status = QueueItemStatus.Completed;
            this.workerExpires = 0;
            this.completedTime = Utils.ToUnixTime(DateTime.UtcNow);
        }

        public void SetAsPending()
        {
            this.status = QueueItemStatus.Pending;
            this.workerExpires = 0;
            this.currentWorker = null;
            this.processStartTime = 0;
        }
    }
}
