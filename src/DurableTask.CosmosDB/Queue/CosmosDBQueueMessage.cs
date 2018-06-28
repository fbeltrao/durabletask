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

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Cosmos DB queue message
    /// </summary>
    class CosmosDbQueueMessage : IQueueMessage
    {
        [JsonIgnore]
        public DateTimeOffset? InsertionTime => Utils.FromUnixTime(this.CreatedDate);

        public long CreatedDate { get; set; }

        [JsonIgnore]
        DateTimeOffset? IQueueMessage.NextVisibleTime => Utils.FromUnixTime(this.NextVisibleTime);

        public long NextVisibleTime { get; set; }
        
        public long LockedUntil { get; set; }

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
