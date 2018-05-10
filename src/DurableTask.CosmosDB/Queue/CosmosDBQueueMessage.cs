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
        [JsonIgnore]
        public DateTimeOffset? InsertionTime => Utils.FromUnixTime(this.CreatedDate);

        public long CreatedDate { get; set; }

        [JsonIgnore]
        DateTimeOffset? IQueueMessage.NextVisibleTime => Utils.FromUnixTime(this.NextVisibleTime.Value);

        public long? NextVisibleTime { get; set; }
        
        public long? LockedUntil { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }
        
        public int DequeueCount { get; set; }

        public object Data { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public QueueItemStatus Status { get; set; }


        [JsonIgnore]
        public string ETag { get; set; }

        public string QueueName { get;  set; }
        
        // used to set expiration policy
        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? TimeToLive { get; set; }

        public string AsString()
        {
            return JsonConvert.SerializeObject(this);
        }        
    }
}
