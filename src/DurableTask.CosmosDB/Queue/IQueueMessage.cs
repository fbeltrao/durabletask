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

using System;

namespace DurableTask.CosmosDB.Queue
{
    /// <summary>
    /// Represents a queue message
    /// </summary>
    public interface IQueueMessage
    {
        /// <summary>
        /// Insert time
        /// </summary>
        DateTimeOffset? InsertionTime { get; }

        /// <summary>
        /// Identifier
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Dequeue count 
        /// </summary>
        int DequeueCount { get;  }

        /// <summary>
        /// Next visible time
        /// </summary>
        DateTimeOffset? NextVisibleTime { get; }

        /// <summary>
        /// Message as string
        /// </summary>
        /// <returns></returns>
        string AsString();
    }
}
