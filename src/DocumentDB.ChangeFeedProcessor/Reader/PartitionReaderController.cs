//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class PartitionReaderController : IPartitionReaderController
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly ConcurrentDictionary<string, IPartitionReader> currentlyOwnedPartitions = new ConcurrentDictionary<string, IPartitionReader>();

        private readonly ILeaseManager leaseManager;
        private readonly IPartitionReaderFactory partitionReaderFactory;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly CancellationTokenSource shutdownCts = new CancellationTokenSource();

        public PartitionReaderController(ILeaseManager leaseManager, IPartitionReaderFactory partitionReaderFactory, IPartitionSynchronizer synchronizer)
        {
            this.leaseManager = leaseManager;
            this.partitionReaderFactory = partitionReaderFactory;
            this.synchronizer = synchronizer;
        }

        public async Task InitializeAsync()
        {
            await this.LoadLeasesAsync().ConfigureAwait(false);
        }

        public async Task AddOrUpdateLeaseAsync(ILease lease)
        {
            var newPartitionReader = this.partitionReaderFactory.Create(lease);
            if (!this.currentlyOwnedPartitions.TryAdd(lease.PartitionId, newPartitionReader))
            {
                await this.leaseManager.UpdatePropertiesAsync(lease).ConfigureAwait(false);
                Logger.DebugFormat("partition {0}: updated", lease.PartitionId);
                return;
            }
            else
            {
                await newPartitionReader.StartAsync(this.shutdownCts.Token).ConfigureAwait(false);
            }

            try
            {
                var updatedLease = await this.leaseManager.AcquireAsync(lease).ConfigureAwait(false);
                if (updatedLease != null) lease = updatedLease;
                Logger.InfoFormat("partition {0}: acquired", lease.PartitionId);
            }
            catch (Exception)
            {
                await this.RemoveLeaseAsync(lease).ConfigureAwait(false);
                throw;
            }
        }

        public Task ShutdownAsync()
        {
            this.shutdownCts.Cancel();

            // IEnumerable<Task> leases = this.currentlyOwnedPartitions.Select(pair => pair.Value..Task).ToList();
            // await Task.WhenAll(leases).ConfigureAwait(false);
            return Task.FromResult(0);
        }

        public async Task<ChangeFeedDocumentChanges> ReadAsync()
        {
            var readTasks = new List<Task<ChangeFeedDocumentChanges>>();

            foreach (var kv in this.currentlyOwnedPartitions)
            {
                if (kv.Value != null)
                {
                    readTasks.Add(kv.Value.ReadAsync());
                }
            }

            await Task.WhenAll(readTasks).ConfigureAwait(false);

            return ChangeFeedDocumentChanges.Combine(readTasks.Select(x => x.Result));
        }

        private async Task LoadLeasesAsync()
        {
            Logger.Debug("Starting renew leases assigned to this host on initialize.");
            var addLeaseTasks = new List<Task>();
            foreach (ILease lease in await this.leaseManager.ListOwnedLeasesAsync().ConfigureAwait(false))
            {
                Logger.InfoFormat("Acquired lease for PartitionId '{0}' on startup.", lease.PartitionId);
                addLeaseTasks.Add(this.AddOrUpdateLeaseAsync(lease));
            }

            await Task.WhenAll(addLeaseTasks.ToArray()).ConfigureAwait(false);
        }

        private async Task RemoveLeaseAsync(ILease lease)
        {
            if (!this.currentlyOwnedPartitions.TryRemove(lease.PartitionId, out var reader))
            {
                return;
            }

            Logger.InfoFormat("partition {0}: released", lease.PartitionId);

            try
            {
                await this.leaseManager.ReleaseAsync(lease).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.WarnException("partition {0}: failed to remove lease", e, lease.PartitionId);
            }
        }

        private async Task HandleSplitAsync(ILease lease, string lastContinuationToken)
        {
            try
            {
                lease.ContinuationToken = lastContinuationToken;
                IEnumerable<ILease> addedLeases = await this.synchronizer.SplitPartitionAsync(lease).ConfigureAwait(false);
                Task[] addLeaseTasks = addedLeases.Select(l =>
                    {
                        l.Properties = lease.Properties;
                        return this.AddOrUpdateLeaseAsync(l);
                    }).ToArray();

                await this.leaseManager.DeleteAsync(lease).ConfigureAwait(false);
                await Task.WhenAll(addLeaseTasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.WarnException("partition {0}: failed to split", e, lease.PartitionId);
            }
        }
    }
}