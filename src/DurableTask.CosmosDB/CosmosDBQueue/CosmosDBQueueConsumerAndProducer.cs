using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace CosmosDBQueue
{
    /// <summary>
    /// Consumer and producer for CosmosDB Queue
    /// </summary>
    public class CosmosDBQueueConsumerAndProducer
    {
        /// <summary>
        /// 
        /// </summary>
        public class QueueTaskMessage : ITaskMessage
        {
            internal CosmosDBQueueItem QueueItem { get; set; }
            private readonly ITaskMessage taskMessage;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="item"></param>
            public QueueTaskMessage(CosmosDBQueueItem item)
            {
                this.QueueItem = item;
                this.taskMessage = (ITaskMessage)item.Data;
            }

            /// <summary>
            /// Event
            /// </summary>
            public HistoryEvent Event
            {
                get { return this.taskMessage.Event; }
                set { this.taskMessage.Event = value; }
            }


            /// <summary>
            /// Sequence number
            /// </summary>
            public long SequenceNumber
            {
                get { return this.taskMessage.SequenceNumber; }
                set { this.taskMessage.SequenceNumber = value; }
            }

            /// <summary>
            /// 
            /// </summary>
            public OrchestrationInstance OrchestrationInstance
            {
                get { return this.taskMessage.OrchestrationInstance; }
                set { this.taskMessage.OrchestrationInstance = value; }

            }


            /// <summary>
            /// 
            /// </summary>
            public ExtensionDataObject ExtensionData
            {
                get { return this.taskMessage.ExtensionData; }
                set { this.taskMessage.ExtensionData = value; }
            }
        }

        CosmosDBQueueConsumer consumer;        
        CosmosDBQueueProducer producer;
        System.Collections.Concurrent.ConcurrentQueue<QueueTaskMessage> cachedItems = new System.Collections.Concurrent.ConcurrentQueue<QueueTaskMessage>();

        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueueConsumerAndProducer()
        {
            
        }

        internal async Task Initialize(CosmosDBCollectionDefinition queueCollectionDefinition, CosmosDBCollectionDefinition leaseCollectionDefinition)
        {
            this.consumer = new CosmosDBQueueConsumer();
            this.consumer.OnMessage = this.MessageHandler;
            var consumerSettings = new CosmosDBQueueConsumerSettings()
            {
                AutoComplete = false,
                QueueCollectionDefinition = queueCollectionDefinition,
                LeaseCollectionDefinition = leaseCollectionDefinition                
            };

            await this.consumer.Start(consumerSettings);


            this.producer = new CosmosDBQueueProducer();
            var producerSettings = new CosmosDBQueueProducerSettings
            {
                QueueCollectionDefinition = queueCollectionDefinition,
                
            };

            await this.producer.Initialize(producerSettings);
        }

        

        private Task<bool> MessageHandler(CosmosDBQueueItem item)
        {
            cachedItems.Enqueue(new QueueTaskMessage(item));
            return Task.FromResult<bool>(true);
        }

        internal async Task SendMessageAsync(ITaskMessage m)
        {
            await this.producer.Queue(m);
        }

        internal Task<ITaskMessage> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken token)
        {
            if (this.cachedItems.TryDequeue(out var queueItem))
            {
                return Task.FromResult<ITaskMessage>(queueItem);
            }

            return null;

        }

        internal async Task AbandonMessageAsync(ITaskMessage taskMessage)
        {
            var qm = (QueueTaskMessage)taskMessage;
            await qm.QueueItem.Abandon();
        }

        internal async Task CompleteMessageAsync(ITaskMessage taskMessage)
        {
            var qm = (QueueTaskMessage)taskMessage;
            await qm.QueueItem.Complete();
        }
    }
}
