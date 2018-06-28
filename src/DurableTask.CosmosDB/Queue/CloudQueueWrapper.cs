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

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Wrapper for <see cref="Microsoft.WindowsAzure.Storage.Queue.CloudQueue"/>
    /// </summary>
    public class CloudQueueWrapper : IQueue
    {
        internal readonly CloudQueue CloudQueue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cloudQueue"></param>
        public CloudQueueWrapper(CloudQueue cloudQueue)
        {
            this.CloudQueue = cloudQueue;
        }

        /// <inheritdoc />
        public string Name => this.CloudQueue.Name;

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            await this.CloudQueue.CreateIfNotExistsAsync();
        }

        /// <inheritdoc />
        public async Task DeleteMessageAsync(IQueueMessage queueMessage, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            await this.CloudQueue.DeleteMessageAsync(((CloudQueueMessageWrapper)queueMessage).CloudQueueMessage, requestOptions, operationContext);
            
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IQueueMessage>> GetMessagesAsync(int controlQueueBatchSize, TimeSpan controlQueueVisibilityTimeout, QueueRequestOptions controlQueueRequestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return (await this.CloudQueue.GetMessagesAsync(
                controlQueueBatchSize,
                controlQueueVisibilityTimeout,
                controlQueueRequestOptions,
                operationContext,
                cancellationToken
                ))
                .Select(x => new CloudQueueMessageWrapper(x));
        }

        /// <inheritdoc />
        public async Task UpdateMessageAsync(IQueueMessage originalQueueMessage, TimeSpan controlQueueVisibilityTimeout, MessageUpdateFields visibility, QueueRequestOptions requestOptions, OperationContext operationContext)
        {
            await this.CloudQueue.UpdateMessageAsync(
                ((CloudQueueMessageWrapper)originalQueueMessage).CloudQueueMessage,
                controlQueueVisibilityTimeout,
                visibility,
                requestOptions,
                operationContext
                );
        }

        /// <inheritdoc />
        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.CloudQueue.DeleteIfExistsAsync();
        }

        /// <inheritdoc />

        public async Task<IQueueMessage> PeekMessageAsync()
        {
            var cloudQueueMessage = await this.CloudQueue.PeekMessageAsync();
            if (cloudQueueMessage != null)
                return new CloudQueueMessageWrapper(cloudQueueMessage);

            return null;
        }

        /// <inheritdoc />

        public async Task<int> GetQueueLenghtAsync()
        {
            await this.CloudQueue.FetchAttributesAsync();
            return this.CloudQueue.ApproximateMessageCount.GetValueOrDefault(0);
        }

        /// <inheritdoc />

        public async Task<bool> ExistsAsync()
        {
            return await this.CloudQueue.ExistsAsync();
        }
    }
}
