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

namespace DurableTask.Core
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents the state of an orchestration
    /// </summary>
    [DataContract]
    public class OrchestrationState : IExtensibleDataObject
    {
        /// <summary>
        /// Completion datetime of the orchestration
        /// </summary>
        [JsonProperty("completedTime")]
        [DataMember] public DateTime CompletedTime;

        /// <summary>
        /// The size of the compressed serialized runtimestate
        /// </summary>
        [JsonProperty("compressedSize")]
        [DataMember] public long CompressedSize;

        /// <summary>
        /// Creation time of the orchestration
        /// </summary>
        [JsonProperty("createdTime")]
        [DataMember] public DateTime CreatedTime;

        /// <summary>
        /// Serialized input of the orchestration
        /// </summary>
        [JsonProperty("input")]
        [DataMember] public string Input;

        /// <summary>
        /// Last updated time of the orchestration
        /// </summary>
        [JsonProperty("lastUpdatedTime")]
        [DataMember] public DateTime LastUpdatedTime;

        /// <summary>
        /// The orchestration name
        /// </summary>
        [JsonProperty("name")]
        [DataMember] public string Name;

        /// <summary>
        /// The orchestration instance this state represents
        /// </summary>
        [JsonProperty("orchestrationInstance")]
        [DataMember] public OrchestrationInstance OrchestrationInstance;

        /// <summary>
        /// The current orchestration status
        /// </summary>
        [JsonProperty("orchestrationStatus")]
        [JsonConverter(typeof(StringEnumConverter))]
        [DataMember] public OrchestrationStatus OrchestrationStatus;

        /// <summary>
        /// The serialized output of the orchestration
        /// </summary>
        [JsonProperty("output")]
        [DataMember] public string Output;

        /// <summary>
        /// The parent instance if this is orchestration has one
        /// </summary>
        [JsonProperty("parentInstance", DefaultValueHandling = DefaultValueHandling.Ignore)]
        [DataMember] public ParentInstance ParentInstance;

        /// <summary>
        /// The size of the raw (uncompressed) serialized runtimestate
        /// </summary>
        [JsonProperty("size")]
        [DataMember] public long Size;

        /// <summary>
        /// String status of the orchestration
        /// </summary>
        [JsonProperty("status", DefaultValueHandling = DefaultValueHandling.Ignore)]
        [DataMember] public string Status;

        /// <summary>
        /// The dictionary of tags and string values associated with this orchestration
        /// </summary>
        [JsonProperty("tags", DefaultValueHandling = DefaultValueHandling.Ignore)]
        [DataMember] public IDictionary<string, string> Tags;

        /// <summary>
        /// The orchestration version
        /// </summary>
        [DefaultValue("")]
        [JsonProperty("version", DefaultValueHandling = DefaultValueHandling.Ignore)]
        [DataMember] public string Version;

        /// <summary>
        /// Implementation for <see cref="IExtensibleDataObject.ExtensionData"/>.
        /// </summary>
        [JsonProperty("extensionData", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public ExtensionDataObject ExtensionData { get; set; }
    }
}