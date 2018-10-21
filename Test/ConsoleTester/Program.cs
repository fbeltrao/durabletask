using DurableTask.Core;
using System;
using System.Threading.Tasks;
using System.Diagnostics;
using Serilog;

namespace DurableTask.ConsoleTester
{
    class Program
    {
        static TestOrchestrationHost CreateOrchestrationHost(string workerId, OrchestrationBackendType backendType)
        {
            var host = TestHelpers.GetTestOrchestrationHost(backendType, workerId);
            host.StartAsync().GetAwaiter().GetResult();
            host.RegisterOrchestrationTypes(typeof(ParallelActivityProcessing));
            return host;
        }

        static async Task Main(string[] args)
        {
            //var listener = new global::SerilogTraceListener.SerilogTraceListener();
            //Trace.Listeners.Add(listener);

            //Log.Logger = new LoggerConfiguration()
            //    .WriteTo.Console()
            //    .CreateLogger();

            


            var isWorker = (args.Length >= 2 && args[1].ToLower() == "worker");
            var workerId = (isWorker && args.Length >= 3) ? args[2] : null;

            var backendTypeParameterValue = (args.Length >= 1) ? args[0] : "cosmosdb";

            var backendType = OrchestrationBackendType.CosmosDB;
            switch (backendTypeParameterValue.ToLower())
            {
                case "storage":
                    backendType = OrchestrationBackendType.Storage;
                    break;

                case "sql":
                    backendType = OrchestrationBackendType.SQL;
                    break;
            }

            var host = CreateOrchestrationHost(workerId, backendType);

            var instancesCount = 3;
            if (args.Length >= 2)
                int.TryParse(args[1], out instancesCount);


            //var host = StorageOrchestrationHost.Value;


            try
            {
                Console.WriteLine($"[{DateTime.Now.ToString()}] Starting {backendType} id={workerId}...");
                if (!isWorker)
                {
                    var stopwatch = Stopwatch.StartNew();
                    var tasks = new Task[instancesCount];
                    for (var i = 0; i < instancesCount; ++i)
                    {
                        tasks[i] = ParallelActivityProcessing(host, 10);
                    }


                    await Task.WhenAll(tasks);
                    stopwatch.Stop();
                    Console.WriteLine($"[{DateTime.Now.ToString()}] Finished {host.GetType().Name} in {stopwatch.ElapsedMilliseconds}ms");
                }
                else
                {
                    Console.WriteLine($"[{DateTime.Now.ToString()}] Press <ENTER> to exit");
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
            TestOrchestrationClient client = null;

            try
            {
                client = await host.StartOrchestrationAsync(typeof(ParallelActivityProcessing), activityCount);
            }
            catch (Exception ex)
            {
                Log.Logger.Error(ex, $"Unexpected error starting orchestration");
                return;
            }

            while (true)
            {

                await Task.Delay(1000 * 5); // wait 5 seconds

                try
                {
                    var stop = false;

                    var status = await client.GetStatusAsync();
                    if (status != null)
                    {
                        switch (status.OrchestrationStatus)
                        {
                            case OrchestrationStatus.Completed:
                                stop = true;
                                break;

                            case OrchestrationStatus.Running:
                            case OrchestrationStatus.Pending:
                                break;

                            default:
                                {
                                    stop = true;
                                    Log.Logger.Error($"Unexpected status : {status.ToString()}");
                                    break;
                                }
                        }
                    }
                    else
                    {
                        Log.Logger.Error($"Unexpected status == null");
                        stop = true;
                    }

                    if (stop)
                        break;
                }
                catch (Exception ex)
                {
                    Log.Logger.Error($"Error getting orchestration status: {ex.ToString()}");
                }
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
