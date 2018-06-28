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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.CosmosDB.Queue;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// Context associated with action triggered by a message in an Azure storage queue.
    /// </summary>
    class ReceivedMessageContext
    {
        static readonly ConcurrentDictionary<object, ReceivedMessageContext> ObjectAssociations;
        static readonly string UserAgent;

        /// <summary>
        /// Message identifier
        /// </summary>
        internal string MessageId { get; private set; }

        readonly string storageAccountName;
        readonly string taskHub;
        readonly Guid traceActivityId;
        readonly MessageData messageData;
        readonly List<MessageData> messageDataBatch;

        static ReceivedMessageContext()
        {
            // All requests to storage will contain the assembly name and version in the User-Agent header.
            Assembly currentAssembly = typeof(ReceivedMessageContext).Assembly;
            FileVersionInfo fileInfo = FileVersionInfo.GetVersionInfo(currentAssembly.Location);
            var assemblyFileVersion = new Version(
                fileInfo.FileMajorPart,
                fileInfo.FileMinorPart,
                fileInfo.FileBuildPart,
                fileInfo.FilePrivatePart);

            UserAgent = $"{currentAssembly.GetName().Name}/{assemblyFileVersion}";

            ObjectAssociations = new ConcurrentDictionary<object, ReceivedMessageContext>();
        }

        ReceivedMessageContext(string messageId, string storageAccountName, string taskHub, Guid traceActivityId, MessageData message)
        {
            this.MessageId = messageId;
            this.storageAccountName = storageAccountName ?? throw new ArgumentNullException(nameof(storageAccountName));
            this.taskHub = taskHub ?? throw new ArgumentNullException(nameof(taskHub));
            this.traceActivityId = traceActivityId;
            this.messageData = message ?? throw new ArgumentNullException(nameof(message));

            this.StorageOperationContext = new OperationContext();
            this.StorageOperationContext.ClientRequestID = traceActivityId.ToString();
        }

        ReceivedMessageContext(string storageAccountName, string taskHub, Guid traceActivityId, List<MessageData> messageBatch)
        {
            this.storageAccountName = storageAccountName ?? throw new ArgumentNullException(nameof(storageAccountName));
            this.taskHub = taskHub ?? throw new ArgumentNullException(nameof(taskHub));
            this.traceActivityId = traceActivityId;
            this.messageDataBatch = messageBatch ?? throw new ArgumentNullException(nameof(messageBatch));

            this.StorageOperationContext = new OperationContext();
            this.StorageOperationContext.ClientRequestID = traceActivityId.ToString();
        }

        public MessageData MessageData => this.messageData;

        public IReadOnlyList<MessageData> MessageDataBatch => this.messageDataBatch;

        public OrchestrationInstance Instance => this.messageData != null ? this.messageData.TaskMessage.OrchestrationInstance : this.messageDataBatch[0].TaskMessage.OrchestrationInstance;

        public OperationContext StorageOperationContext { get; }

        public static ReceivedMessageContext CreateFromReceivedMessageBatch(string storageAccountName, string taskHub, List<MessageData> batch)
        {
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            if (batch.Count == 0)
            {
                throw new ArgumentException("The list must not be empty.", nameof(batch));
            }

            Guid newTraceActivityId = StartNewLogicalTraceScope();
            batch.ForEach(m => TraceMessageReceived(storageAccountName, taskHub, m));

            return new ReceivedMessageContext(storageAccountName, taskHub, newTraceActivityId, batch);
        }

        public static async Task<ReceivedMessageContext> CreateFromReceivedMessageAsync(
            MessageManager messageManager,
            string storageAccountName,
            string taskHub,
            CloudQueueMessage queueMessage,
            string queueName)
        {
            MessageData data = await messageManager.DeserializeQueueMessageAsync(queueMessage, queueName);

            Guid newTraceActivityId = StartNewLogicalTraceScope();
            TraceMessageReceived(storageAccountName, taskHub, data);            

            return new ReceivedMessageContext(queueMessage.Id, storageAccountName, taskHub, newTraceActivityId, data);
        }

        public static ReceivedMessageContext CreateFromReceivedMessage(
            MessageData data,
            string storageAccountName,
            string taskHub,
            string queueMessageId,
            string queueName)
        {            
            Guid newTraceActivityId = StartNewLogicalTraceScope();
            TraceMessageReceived(storageAccountName, taskHub, data);

            return new ReceivedMessageContext(queueMessageId, storageAccountName, taskHub, newTraceActivityId, data);
        }
        static Guid StartNewLogicalTraceScope()
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            Guid newTraceActivityId = Guid.NewGuid();
            AnalyticsEventSource.SetLogicalTraceActivityId(newTraceActivityId);
            return newTraceActivityId;
        }

        static void TraceMessageReceived(string storageAccountName, string taskHub, MessageData data)
        {
            TaskMessage taskMessage = data.TaskMessage;
            IQueueMessage queueMessage = data.OriginalQueueMessage;

            AnalyticsEventSource.Log.ReceivedMessage(
                data.ActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.Id,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertionTime.Value).TotalMilliseconds),
                queueMessage.DequeueCount,
                data.TotalMessageSizeBytes,
                PartitionId: data.QueueName);
        }

        public DateTime GetNextMessageExpirationTimeUtc()
        {
            if (this.messageData != null)
            {
                return this.messageData.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime;
            }
            else
            {
                return this.messageDataBatch.Min(msg => msg.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime);
            }
        }

        public bool TrySave(object relatedObject)
        {
            if (relatedObject == null)
            {
                throw new ArgumentNullException(nameof(relatedObject));
            }

            return ObjectAssociations.TryAdd(relatedObject, this);
        }

        public static bool TryRestoreContext(object relatedObject, out ReceivedMessageContext context)
        {
            if (!ObjectAssociations.TryGetValue(relatedObject, out context))
            {
                return false;
            }

            RestoreContext(context);
            return true;
        }

        public static bool RemoveContext(object relatedObject)
        {
            ReceivedMessageContext ignoredContext;
            return ObjectAssociations.TryRemove(relatedObject, out ignoredContext);
        }

        static void RestoreContext(ReceivedMessageContext context)
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            AnalyticsEventSource.SetLogicalTraceActivityId(context.traceActivityId);
        }
    }
}
