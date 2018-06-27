﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.CosmosDB.Monitoring
{
    /// <summary>
    /// Data structure containing scale-related statistics for the work-item queue.
    /// </summary>
    public struct WorkItemQueueData
    {
        /// <summary>
        /// Gets or sets the number of messages in the work-item queue.
        /// </summary>
        public int QueueLength { get; internal set; }

        /// <summary>
        /// Gets or sets the age of the first message in the work-item queue.
        /// </summary>
        public TimeSpan FirstMessageAge { get; internal set; }
    }
}
