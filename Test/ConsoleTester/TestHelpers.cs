using DurableTask.AzureStorage;
using DurableTask.CosmosDB;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.ConsoleTester
{
    public static class TestHelpers
    {
        const string LeaseManagementCollectionName = "unitTestLeaseManagement";

        public static TestOrchestrationHost GetTestOrchestrationHost(OrchestrationBackendType orchestrationBackendType = OrchestrationBackendType.Storage, string workerId = null)
        {
            string storageConnectionString = orchestrationBackendType == OrchestrationBackendType.Storage ? GetTestStorageAccountConnectionString() : null;
            var leaseManagementCollection = orchestrationBackendType == OrchestrationBackendType.Storage ? null : LeaseManagementCollectionName;
            var settings = new StorageOrchestrationServiceSettings
            {
                StorageConnectionString = storageConnectionString,
                TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName"),
                ApplicationInsightsInstrumentationKey = ConfigurationManager.AppSettings.Get("APPINSIGHTS_INSTRUMENTATIONKEY"),
            };

            switch (orchestrationBackendType)
            {
                case OrchestrationBackendType.SQL:
                    {
                        settings.SqlConnectionString = ConfigurationManager.AppSettings.Get("SqlConnectionString");
                        settings.SqlQueueUseMemoryOptimizedTable = ConfigurationManager.AppSettings.Get("SqlQueueUseMemoryOptimizedTable")?.ToLower() == "true";


                    }
                    break;

                case OrchestrationBackendType.CosmosDB:
                    {
                        settings.CosmosDBAuthKey = ConfigurationManager.AppSettings.Get("CosmosDBAuthKey");
                        settings.CosmosDBEndpoint = ConfigurationManager.AppSettings.Get("CosmosDBEndpoint");
                        settings.CosmosDBQueueUsePartition = ConfigurationManager.AppSettings.Get("CosmosDBQueueUsePartition")?.ToLower() == "true";
                        settings.CosmosDBQueueUseOneCollectionPerQueueType = ConfigurationManager.AppSettings.Get("CosmosDBQueueUseOneCollectionPerQueueType")?.ToLower() == "true";
                        settings.CosmosDBLeaseManagementUsePartition = ConfigurationManager.AppSettings.Get("CosmosDBLeaseManagementUsePartition")?.ToLower() == "true";
                        settings.CosmosDBLeaseManagementCollection = leaseManagementCollection;

                        if (int.TryParse(ConfigurationManager.AppSettings.Get("CosmosDBQueueCollectionThroughput"), out var cosmosDBQueueCollectionThroughput))
                        {
                            if (cosmosDBQueueCollectionThroughput >= 400)
                                settings.CosmosDBQueueCollectionThroughput = cosmosDBQueueCollectionThroughput;
                        }

                        break;
                    }
            }

            if (int.TryParse(ConfigurationManager.AppSettings.Get("PartitionCount"), out var partitionCount))
            {
                settings.PartitionCount = partitionCount;
            }

            if (!string.IsNullOrEmpty(workerId))
                settings.WorkerId = workerId;




            var service = new ExtensibleOrchestrationService(settings);

            service.CreateAsync(false).GetAwaiter().GetResult();

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
