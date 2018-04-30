using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace DurableTask.CosmosDB.Queue
{
    internal class CloudQueueMessageWrapper : IQueueMessage
    {
        internal CloudQueueMessage CloudQueueMessage { get; }

        public DateTimeOffset? InsertionTime => CloudQueueMessage.InsertionTime;

        public string Id => CloudQueueMessage.Id;

        public int DequeueCount => CloudQueueMessage.DequeueCount;

        public DateTimeOffset? NextVisibleTime => CloudQueueMessage.NextVisibleTime;

        public string AsString()
        {
            return this.CloudQueueMessage.AsString;
        }

        internal CloudQueueMessageWrapper(CloudQueueMessage cloudQueueMessage)
        {
            CloudQueueMessage = cloudQueueMessage;
        }

        
    }
}
