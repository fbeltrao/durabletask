using System;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.CosmosDB.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.CosmosDB.Performance.Tests
{
    [TestClass]
    public class CosmosDBTests
    {
        static TestOrchestrationHost cosmosDBHost;
        static CosmosDBTests()
        {
            cosmosDBHost = TestHelpers.GetTestOrchestrationHost(OrchestrationBackendType.CosmosDB);
            cosmosDBHost.StartAsync().GetAwaiter().GetResult();
        }

        [TestMethod]
        public async Task CosmosDB_ParallelActivityProcessing()
        {
            var client = await cosmosDBHost.StartOrchestrationAsync(typeof(ParallelActivityProcessing), Constants.ACTIVITY_COUNT);            
            
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(5));
            while (status?.OrchestrationStatus != OrchestrationStatus.Completed)
            {
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(2));
            }

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Input.ToString());
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Output.ToString());           
        }

        [TestMethod]
        public async Task CosmosDB_SequentialActivityProcessing()
        {
            var client = await cosmosDBHost.StartOrchestrationAsync(typeof(SequentialActivityProcessing), Constants.ACTIVITY_COUNT);

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(5));
            while (status?.OrchestrationStatus != OrchestrationStatus.Completed)
            {
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(2));
            }

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Input.ToString());
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Output.ToString());
        }

        [TestMethod]
        public void ActorOrchestration()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.ActorOrchestration(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void CosmosDBQueueMessage_DeserializationArrayTest()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.CosmosDBQueueMessage_DeserializationArrayTest();
        }

        [TestMethod]
        public void CosmosDBQueueMessage_DeserializationSingleTest()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.CosmosDBQueueMessage_DeserializationSingleTest();
        }

        [TestMethod]
        public void CosmosDBQueueMessage_DeserializeExecutionStartedEvent()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.CosmosDBQueueMessage_DeserializeExecutionStartedEvent();
        }

        [TestMethod]
        public void CosmosDBQueueMessage_DeserializeTaskMessage()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.CosmosDBQueueMessage_DeserializeTaskMessage();
        }

        [TestMethod]
        public void FanOutToTableStorage()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.FanOutToTableStorage(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void HelloWorldOrchestration_Activity()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.HelloWorldOrchestration_Activity(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void HelloWorldOrchestration_Inline()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.HelloWorldOrchestration_Inline(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void LargeBinaryByteMessagePayloads()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.LargeBinaryByteMessagePayloads(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void LargeBinaryStringMessagePayloads()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.LargeBinaryStringMessagePayloads(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void LargeTextMessagePayloads()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.LargeTextMessagePayloads(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void OrchestrationConcurrency()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.OrchestrationConcurrency(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void ParallelOrchestration()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.ParallelOrchestration(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void RecreateCompletedInstance()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.RecreateCompletedInstance(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void RecreateFailedInstance()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.RecreateFailedInstance(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void RecreateTerminatedInstance()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.RecreateTerminatedInstance(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void SequentialOrchestration()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.SequentialOrchestration(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void SmallTextMessagePayloads()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.SmallTextMessagePayloads(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void TerminateOrchestration()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.TerminateOrchestration().Wait();
        }

        [TestMethod]
        public void TimerCancellation()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.TimerCancellation().Wait();
        }

        [TestMethod]
        public void TimerExpiration()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.TimerExpiration().Wait();
        }

        [TestMethod]
        public void TryRecreateRunningInstance()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.TryRecreateRunningInstance(OrchestrationBackendType.CosmosDB).Wait();
        }

        [TestMethod]
        public void UnhandledActivityException()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.UnhandledActivityException().Wait();
        }

        [TestMethod]
        public void UnhandledOrchestrationException()
        {
            AzureStorageScenarioTests testHost = new AzureStorageScenarioTests();
            testHost.UnhandledOrchestrationException().Wait();
        }
    }
}
