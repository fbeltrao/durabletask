using DurableTask.Core;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace DurableTask.ConsoleTester
{
    /// <summary>
    /// Sequential processing of activities
    /// Will evaluate how fast orchestration is loaded and updated based on on sequential activity execution
    /// </summary>
    [KnownType(typeof(NumberPlusOneActivity))]
    public class SequentialActivityProcessing : TaskOrchestration<int, int>
    {
        public class NumberPlusOneActivity : TaskActivity<int, int>
        {
            protected override int Execute(TaskContext context, int number)
            {
                return number + 1;
            }
        }

        public override async Task<int> RunTask(OrchestrationContext context, int taskCount)
        {
            var lastResult = 0;
            for (int i = 0; i < taskCount; i++)
            {
                lastResult = await context.ScheduleTask<int>(typeof(NumberPlusOneActivity), i);
            }


            return lastResult;
        }
    }
}
