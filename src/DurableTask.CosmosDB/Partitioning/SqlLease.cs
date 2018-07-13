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

using DurableTask.AzureStorage.Partitioning;
using Newtonsoft.Json;
using System;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// SqlLease lease
    /// </summary>
    class SqlLease : Lease
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public SqlLease()
        {

        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="source"></param>
        public SqlLease(SqlLease source) : base(source)
        {
            this.TaskHubName = source.TaskHubName;
            this.LeaseTimeout = source.LeaseTimeout;
        }      

        /// <summary>
        /// Task hub name
        /// </summary>
        public string TaskHubName { get; set; }


        /// <summary>
        /// Lease timeout
        /// </summary>
        public long LeaseTimeout { get; set; }

        /// <inheritdoc />
        public override bool IsExpired()
        {
            var now = Utils.ToUnixTime(DateTime.UtcNow);
            return this.LeaseTimeout < now;
        }
    }
}