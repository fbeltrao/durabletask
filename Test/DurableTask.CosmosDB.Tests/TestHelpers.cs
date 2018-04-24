using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.CosmosDB.Tests
{
    static class TestHelpers
    {

        public static CosmosDBOrchestrationService GetAndCreateCosmosDBOrchestrationService()
        {
            string cosmosEndpoint = GetTestCosmosDBEndpoint();
            var cosmosAuthKey = GetTestCosmosDBAuthKey();

            var service = new CosmosDBOrchestrationService(
               new CosmosDBOrchestrationServiceSettings
               {
                   CosmosDBEndpoint = cosmosEndpoint,
                   CosmosDBAuthKey = cosmosAuthKey,
                   TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName"),
               });

            service.CreateAsync().GetAwaiter().GetResult();

            return service;
        }

        public static TestOrchestrationHost GetTestOrchestrationHost()
        {
            var orchService = GetAndCreateCosmosDBOrchestrationService();

            return new TestOrchestrationHost(orchService);
        }

        public static string GetTestCosmosDBEndpoint()
        {
            string endpoint = GetTestSetting("CosmosDBEndpoint");
            if (string.IsNullOrEmpty(endpoint))
            {
                throw new ArgumentNullException("A CosmosDB endpoint must be defined in either an environment variable or in configuration.");
            }

            return endpoint;
        }

        public static string GetTestCosmosDBAuthKey()
        {
            string authKey = GetTestSetting("CosmosDBAuthKey");
            if (string.IsNullOrEmpty(authKey))
            {
                throw new ArgumentNullException("A CosmosDB authorization key must be defined in either an environment variable or in configuration.");
            }

            return authKey;
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
