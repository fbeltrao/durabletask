using System;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Partitioning
{
    interface IPartitionManager
    {
        Task InitializeAsync();
        Task StartAsync();
        Task StopAsync();
        Task<IDisposable> SubscribeAsync(IPartitionObserver observer);
        Task TryReleasePartitionAsync(string partitionId, string leaseToken);
    }
}