//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class PartitionReaderSupervisorFactory : IPartitionSupervisorFactory
    {
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedProcessorOptions changeFeedProcessorOptions;
        private readonly IPartitionReaderFactory partitionReaderFactory;

        public PartitionReaderSupervisorFactory(
            ILeaseManager leaseManager,
            IPartitionReaderFactory partitionReaderFactory,
            ChangeFeedProcessorOptions options)
        {
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (partitionReaderFactory == null) throw new ArgumentNullException(nameof(partitionReaderFactory));

            this.leaseManager = leaseManager;
            this.changeFeedProcessorOptions = options;
            this.partitionReaderFactory = partitionReaderFactory;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            var reader = this.partitionReaderFactory.Create(lease);
            var renewer = new LeaseRenewer(lease, this.leaseManager, this.changeFeedProcessorOptions.LeaseRenewInterval);

            return new PartitionReaderSupervisor(lease, reader, renewer);
        }
    }
}