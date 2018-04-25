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
    using DurableTask.Core;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    internal class PeeklockQueue
    {
        CosmosDBQueue messagesInCosmos;
        //List<TaskMessage> messages;
        HashSet<TaskMessage> lockTable;

        readonly object thisLock = new object();


        public PeeklockQueue()
        {
            //this.messages = new List<TaskMessage>();
            this.lockTable = new HashSet<TaskMessage>();

            messagesInCosmos = new CosmosDBQueue();
            
        }

        internal async Task CreateAsync(CosmosDBQueueSettings queueSettings)
        {
            await messagesInCosmos.Initialize(queueSettings);
        }



        public async Task<CosmosDBQueueItem<TaskMessage>> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            // get next item
            
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                var queueItem = await this.messagesInCosmos.Dequeue<TaskMessage>();
                var tm = queueItem.data;
                if (!this.lockTable.Contains(tm))
                {
                    this.lockTable.Add(tm);
                    return queueItem;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            return null;
        }

        public async Task SendMessageAsync(TaskMessage message)
        {
            //lock(this.thisLock)
            //{
                //this.messages.Add(message);

                // add queue item in cosmos
                await messagesInCosmos.Queue(message);
            //}
        }

        public async Task CompleteMessageAsync(string id, TaskMessage message)
        {
            //lock(this.thisLock)
            //{
            //    if(!this.lockTable.Contains(message))
            //    {
            //        throw new InvalidOperationException("Message Lock Lost");
            //    }

                this.lockTable.Remove(message);
              //  this.messages.Remove(message);
                await this.messagesInCosmos.CompleteAsync(id);
            //}
        }

        public void AbandonMessageAsync(TaskMessage message)
        {
            lock (this.thisLock)
            {
                if (!this.lockTable.Contains(message))
                {
                    return;
                }

                this.lockTable.Remove(message);
            }
        }

        
    }
}