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
using System.Configuration;

namespace DurableTask.CosmosDB.Tests
{
    static class TestHelpers
    {
        const string LeaseManagementCollectionName = "unitTestLeaseManagement";

        public static TestOrchestrationHost GetTestOrchestrationHost(OrchestrationBackendType orchestrationBackendType = OrchestrationBackendType.Storage)
        {
            string storageConnectionString = orchestrationBackendType == OrchestrationBackendType.Storage ? GetTestStorageAccountConnectionString() : null;
            var leaseManagementCollection = orchestrationBackendType == OrchestrationBackendType.Storage ? null : LeaseManagementCollectionName;

            var service = new ExtensibleOrchestrationService(
                new StorageOrchestrationServiceSettings
                {
                    StorageConnectionString = storageConnectionString,
                    TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName"),
                    CosmosDbAuthKey = ConfigurationManager.AppSettings.Get("CosmosDBAuthKey"),
                    CosmosDbEndpoint = ConfigurationManager.AppSettings.Get("CosmosDBEndpoint"),
                    CosmosDbLeaseManagementCollection = leaseManagementCollection,
                });

            service.CreateAsync().GetAwaiter().GetResult();

            return new TestOrchestrationHost(service);
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException("A Storage connection string must be defined in either an environment variable or in configuration.");
            }

            return storageConnectionString;
        }

        static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }

            return value;
        }
    }
}
