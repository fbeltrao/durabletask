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

        SemaphoreSlim thisLock = new SemaphoreSlim(1, 1);
        //readonly object thisLock = new object();


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
                await thisLock.WaitAsync();
                try
                {
                    var queueItem = await this.messagesInCosmos.Dequeue<TaskMessage>();
                    var tm = queueItem?.data;
                    if (tm != null && !this.lockTable.Contains(tm))
                    {
                        this.lockTable.Add(tm);
                        return queueItem;
                    }
                    //await Task.Delay(TimeSpan.FromMilliseconds(500));
                    await Task.Delay(TimeSpan.FromMilliseconds(1000));

                }
                finally
                {
                    thisLock.Release();
                }
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            return null;
        }

        public async Task SendMessageAsync(TaskMessage message)
        {
            await thisLock.WaitAsync();
            try
            {
                await messagesInCosmos.Queue(message);

            }
            finally
            {
                this.thisLock.Release();
            }
                
        }

        public async Task CompleteMessageAsync(string id, TaskMessage message)
        {

            await thisLock.WaitAsync();
            try
            {
                this.lockTable.Remove(message);
                await this.messagesInCosmos.CompleteAsync(id);

            }
            finally
            {
                this.thisLock.Release();
            }
        }

        public void AbandonMessageAsync(TaskMessage message)
        {

            thisLock.Wait();
            try
            {
                if (!this.lockTable.Contains(message))
                {
                    return;
                }

                this.lockTable.Remove(message);
            }
            finally
            {
                this.thisLock.Release();
            }
        }
    }
}