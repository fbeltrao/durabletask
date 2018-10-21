//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class PartitionReaderSupervisor : IPartitionSupervisor
    {
        private readonly ILease lease;
        private readonly IPartitionReader reader;
        private readonly ILeaseRenewer renewer;
        private readonly CancellationTokenSource renewerCancellation = new CancellationTokenSource();
        private CancellationTokenSource processorCancellation;

        public PartitionReaderSupervisor(ILease lease, IPartitionReader reader, ILeaseRenewer renewer)
        {
            this.lease = lease;
            this.reader = reader;
            this.renewer = renewer;
        }

        public async Task RunAsync(CancellationToken shutdownToken)
        {
            var context = new ChangeFeedObserverContext(this.lease.PartitionId);

            this.processorCancellation = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

            Task renewerTask = this.renewer.RunAsync(this.renewerCancellation.Token);
            renewerTask.ContinueWith(_ => this.processorCancellation.Cancel()).LogException();

            ChangeFeedObserverCloseReason closeReason = shutdownToken.IsCancellationRequested ?
                ChangeFeedObserverCloseReason.Shutdown :
                ChangeFeedObserverCloseReason.Unknown;

            try
            {
                await renewerTask.ConfigureAwait(false);
            }
            catch (LeaseLostException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseLost;
                throw;
            }
            catch (PartitionSplitException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseGone;
                throw;
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                closeReason = ChangeFeedObserverCloseReason.Shutdown;
            }
        }

        public void Dispose()
        {
            this.processorCancellation?.Dispose();
            this.renewerCancellation.Dispose();
        }
    }
}