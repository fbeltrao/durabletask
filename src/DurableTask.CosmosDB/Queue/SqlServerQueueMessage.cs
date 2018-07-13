using DurableTask.AzureStorage;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Queue
{
    class SqlServerQueueMessage : IQueueMessage
    {
        static JsonSerializerSettings messageDeserializationSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects
        };
        public long QueueItemID { get; set; }

        public long CreatedDate { get; set; }

        public long NextVisibleTime { get; set; }

        public long NextAvailableTime { get; set; }

        DateTimeOffset? IQueueMessage.InsertionTime => Utils.FromUnixTime(this.CreatedDate);
        DateTimeOffset? IQueueMessage.NextVisibleTime => Utils.FromUnixTime(this.NextVisibleTime);

        string IQueueMessage.Id => QueueItemID.ToString();                

        public int DequeueCount { get; set; }

        public string Data { get; set; }

        public long Version { get; set; }

        public string QueueName { get; set; }

        public int TimeToLive { get; set; }

        public string AsString()
        {
            return JsonConvert.SerializeObject(this);
        }

        private MessageData messageData;

        internal MessageData GetObjectData() => messageData;

        internal void SetObjectData(MessageData messageData)
        {
            this.messageData = messageData ?? throw new ArgumentNullException(nameof(messageData));
        }

        public SqlServerQueueMessage()
        {
        }

        public SqlServerQueueMessage(MessageData messageData)
        {
            this.messageData = messageData;
            this.Data = JsonConvert.SerializeObject(messageData, messageDeserializationSettings);
            //this.Data = JsonConvert.SerializeObject(messageData, new JsonSerializerSettings()
            //{
            //    TypeNameHandling = TypeNameHandling.All
            //});
        }

    }
}
