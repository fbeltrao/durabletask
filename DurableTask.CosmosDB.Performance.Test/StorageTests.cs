using System;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.CosmosDB.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.CosmosDB.Performance.Tests
{
    [TestClass]
    public class StorageTests
    {
        static TestOrchestrationHost storageBasedHost;
        static StorageTests()
        {
            storageBasedHost = TestHelpers.GetTestOrchestrationHost(OrchestrationBackendType.Storage);
            storageBasedHost.StartAsync().GetAwaiter().GetResult();
        }

        [TestMethod]
        public async Task Storage_ParallelActivityProcessing()
        {
            var client = await storageBasedHost.StartOrchestrationAsync(typeof(ParallelActivityProcessing), Constants.ACTIVITY_COUNT);            
            
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
        public async Task Storage_SequentialActivityProcessing()
        {
            var client = await storageBasedHost.StartOrchestrationAsync(typeof(SequentialActivityProcessing), Constants.ACTIVITY_COUNT);

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(5));
            while (status?.OrchestrationStatus != OrchestrationStatus.Completed)
            {
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(2));
            }

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Input.ToString());
            Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Output.ToString());
        }
    }
}
