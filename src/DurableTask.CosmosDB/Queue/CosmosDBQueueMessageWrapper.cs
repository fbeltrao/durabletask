using DocumentDB.Queue;
using DurableTask.AzureStorage;
using DurableTask.Core;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.CosmosDB.Queue
{

    /// <summary>
    /// Message content
    /// </summary>
    public class MessageContent
    {
        /// <summary></summary>
        public object Data { get; set; }

        /// <summary></summary>
        public DateTimeOffset? InsertionTime { get; set; }

        /// <summary></summary>
        public int DequeueCount { get; set; }

        /// <summary></summary>
        public string PartitionKey { get; set; }

        /// <summary></summary>
        public long? NextVisibleTime { get; set; }
        
        /// <summary></summary>
        public long? NextAvailableTime { get; set; }
    }

    /// <summary>
    /// Cosmos DB queue message
    /// </summary>
    internal class CosmosDBQueueMessageWrapper : IQueueMessage
    {
        private readonly CosmosDBQueueMessage internalMessage;
              
        public CosmosDBQueueMessageWrapper()
        {
            this.internalMessage = new CosmosDBQueueMessage();
        }

        internal CosmosDBQueueMessageWrapper(CosmosDBQueueMessage message, MessageContent messageContent)
        {
            this.internalMessage = message;            
            this.contents = messageContent;
        }

        MessageContent contents;
        internal MessageContent Contents
        {
            get
            {
                
                if (this.contents == null)
                {
                    this.contents = this.internalMessage.Data.GetPropertyValue<MessageContent>("data");
                    if (this.contents == null)
                    {
                        this.contents = new MessageContent();
                        this.internalMessage.Data.SetPropertyValue("data", contents);
                    }
                }

                return this.contents;
            }
        }

        internal CosmosDBQueueMessageWrapper SetDataContent(object value)
        {
            this.Contents.Data = value;
            return this;
        }
        internal object GetDataContent() => this.Contents.Data;

        public string Id
        {
            get { return this.internalMessage.Data.Id; }
            set { this.internalMessage.Data.Id = value; }
        }

        public string ETag
        {
            get { return this.internalMessage.Data.ETag; }
        }

        public int? TimeToLive
        {
            get
            {
                return this.internalMessage.Data.TimeToLive;
            }

            set
            {
                this.internalMessage.Data.TimeToLive = value;
            }

        }

        public DateTimeOffset? InsertionTime
        {
            get { return Contents.InsertionTime; }
            set { this.Contents.InsertionTime = value; }
        }

        public int DequeueCount
        {
            get { return Contents.DequeueCount; }
            set { this.Contents.DequeueCount = value; }
        }

        public string PartitionKey
        {
            get { return Contents.PartitionKey; }
            set { this.Contents.PartitionKey = value; }            
        }


        DateTimeOffset? IQueueMessage.NextVisibleTime
        {
            get
            {
                if (this.NextVisibleTime.HasValue)
                    return Utils.FromUnixTime(this.NextVisibleTime.Value);

                return null;
            }
        }

        public long? NextVisibleTime
        {
            get { return Contents.NextVisibleTime; }
            set { this.Contents.NextVisibleTime = value; }
        }

        public long? NextAvailableTime
        {
            get { return Contents.NextAvailableTime; }
            set { this.Contents.NextAvailableTime = value; }
        }

        public string AsString() => JsonConvert.SerializeObject(this.Contents);

        internal void UpdateData(ResourceResponse<Document> res)
        {
            this.contents = null;
            this.internalMessage.Data = res.Resource;

        }

        internal void UpdateData(Document doc)
        {
            this.contents = null;
            this.internalMessage.Data = doc;
        }


        internal CosmosDBQueueMessage GetInternalMessage() => this.internalMessage;
    }
}
