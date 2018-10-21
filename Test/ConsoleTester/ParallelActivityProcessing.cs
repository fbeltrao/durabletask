using DurableTask.Core;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.ConsoleTester
{
    
    /// <summary>
    /// Parallel processing of activities
    /// Will evaluate how fast activities can be executed
    /// </summary>
    [KnownType(typeof(NumberPlusOneActivity))]
    public class ParallelActivityProcessing : TaskOrchestration<int, int>
    {
        static long _count = 0;        

        public class NumberPlusOneActivity : TaskActivity<int, int>
        {
            protected override int Execute(TaskContext context, int number)
            {
                return number + 1;
            }
        }
        public override async Task<int> RunTask(OrchestrationContext context, int taskCount)
        {
            var tasks = new Task<int>[taskCount];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = context.ScheduleTask<int>(typeof(NumberPlusOneActivity), i);
            }

            await Task.WhenAll(tasks);


            var newCount = Interlocked.Increment(ref _count);
            //Log.Logger.Information($"[{DateTime.Now.ToString()}] - ({newCount.ToString("000")}) Orchestration {context.OrchestrationInstance.InstanceId} finished");
            Console.WriteLine($"[{DateTime.Now.ToString()}] - ({newCount.ToString("000")}) Orchestration {context.OrchestrationInstance.InstanceId} finished");
            return tasks[tasks.Length - 1].Result;
        }
    }
}
