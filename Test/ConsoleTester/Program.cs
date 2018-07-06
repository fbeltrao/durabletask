using DurableTask.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace DurableTask.ConsoleTester
{
    class Program
    {
        static TestOrchestrationHost CreateCosmosDBOrchestrationHost(string workerId)
        {
            var host = TestHelpers.GetTestOrchestrationHost(OrchestrationBackendType.CosmosDB, workerId);
            host.StartAsync().GetAwaiter().GetResult();
            host.RegisterOrchestrationTypes(typeof(ParallelActivityProcessing));
            return host;
        }


        static TestOrchestrationHost CreateStorageOrchestrationHost(string workerId)
        {
            var host = TestHelpers.GetTestOrchestrationHost(OrchestrationBackendType.Storage, workerId);
            host.StartAsync().GetAwaiter().GetResult();
            host.RegisterOrchestrationTypes(typeof(ParallelActivityProcessing));
            return host;
        }

        static async Task Main(string[] args)
        {
            var isWorker = (args.Length >= 2 && args[1].ToLower() == "worker");
            var workerId = (isWorker && args.Length >= 3) ? args[2] : null;

            var host = (args.Length >= 1 && args[0] == "storage") ? CreateStorageOrchestrationHost(workerId) : CreateCosmosDBOrchestrationHost(workerId);
            var instancesCount = 3;
            if (args.Length >= 2)                
                int.TryParse(args[1], out instancesCount);
            

            //var host = StorageOrchestrationHost.Value;

            
            try
            {
                Console.WriteLine("Starting...");
                if (!isWorker)
                {
                    var stopwatch = Stopwatch.StartNew();
                    var tasks = new List<Task>();
                    for (var i = 0; i < instancesCount; ++i)
                    {
                        tasks.Add(ParallelActivityProcessing(host, 10));
                    }

                    await Task.WhenAll(tasks);
                    stopwatch.Stop();
                    Console.WriteLine($"Finished {host.GetType().Name} in {stopwatch.ElapsedMilliseconds}ms");
                }
                else
                {
                    Console.WriteLine("Press <ENTER> to exit");
                    Console.ReadLine();
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            if (Debugger.IsAttached && !isWorker)
                Console.ReadLine();
            
        }

        public static async Task ParallelActivityProcessing(TestOrchestrationHost host, int activityCount = Constants.ACTIVITY_COUNT)
        {
            var client = await host.StartOrchestrationAsync(typeof(ParallelActivityProcessing), activityCount);
            await Task.Delay(1000 * 5); // wait 5 seconds

            var status = await client.GetStatusAsync();
            while (status?.OrchestrationStatus != OrchestrationStatus.Completed)
            {
                await Task.Delay(1000 * 5); // wait 5 seconds
                status = await client.GetStatusAsync();
            }

            //Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            //Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Input.ToString());
            //Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Output.ToString());
        }

        public static async Task SequentialActivityProcessing(TestOrchestrationHost host, int activityCount = Constants.ACTIVITY_COUNT)
        {
            var client = await host.StartOrchestrationAsync(typeof(SequentialActivityProcessing), activityCount);

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(5));
            while (status?.OrchestrationStatus != OrchestrationStatus.Completed)
            {
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(2));
            }

            //Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            //Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Input.ToString());
            //Assert.AreEqual(Constants.ACTIVITY_COUNT.ToString(), status?.Output.ToString());
        }

    }
}
