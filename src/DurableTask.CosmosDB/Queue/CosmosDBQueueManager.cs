//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//using DurableTask.AzureStorage;
//using DurableTask.Core;
//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Queue;

//namespace DurableTask.CosmosDB.Queue
//{
//    class CosmosDBQueueManager : IQueueManager
//    {
//        private readonly StorageOrchestrationServiceSettings settings;
//        ConcurrentDictionary<string, IQueue> allControlQueues;
//        ConcurrentDictionary<string, IQueue> ownedControlQueues;
//        IQueue workItemQueue;

//        public CosmosDBQueueManager(StorageOrchestrationServiceSettings settings)
//        {
//            this.settings = settings;
//        }

//        public ConcurrentDictionary<string, IQueue> AllControlQueues => allControlQueues;

//        public ConcurrentDictionary<string, IQueue> OwnedControlQueues => ownedControlQueues;

//        public IQueue WorkItemQueue => workItemQueue;

//        public string StorageName => this.settings.CosmosDBEndpoint;

//        public Task AddMessageAsync(IQueue queue, TaskMessage message, TimeSpan? timeToLive, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions, OperationContext operationContext)
//        {
//            throw new NotImplementedException();
//        }

//        public Task DeleteAsync()
//        {
//            throw new NotImplementedException();
//        }

//        public Task EnqueueMessageAsync(IQueue queue, ReceivedMessageContext context, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions requestOptions)
//        {
//            throw new NotImplementedException();
//        }

//        public IQueue GetControlQueue(int partitionIndex)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IQueue> GetControlQueueAsync(string partitionId)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<IQueue[]> GetControlQueuesAsync(int partitionCount)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<ReceivedMessageContext> GetMessageAsync(IQueue queue, TimeSpan queueVisibilityTimeout, QueueRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
//        {
//            throw new NotImplementedException();
//        }

//        public Task<List<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
//        {
//            throw new NotImplementedException();
//        }

//        public IQueue GetQueue(string partitionId)
//        {
//            throw new NotImplementedException();
//        }

//        public Task StartAsync()
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
