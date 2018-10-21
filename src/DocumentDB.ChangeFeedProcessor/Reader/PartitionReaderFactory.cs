//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// Builder for <see cref="IPartitionReader"/>
    /// </summary>
    internal class PartitionReaderFactory : IPartitionReaderFactory
    {
        private IChangeFeedDocumentClient feedDocumentClient;
        private ChangeFeedProcessorOptions changeFeedProcessorOptions;
        private ILeaseManager leaseManager;
        private string collectionSelfLink;

        public PartitionReaderFactory(IChangeFeedDocumentClient feedDocumentClient, ChangeFeedProcessorOptions changeFeedProcessorOptions, ILeaseManager leaseManager, string collectionSelfLink)
        {
            this.feedDocumentClient = feedDocumentClient;
            this.changeFeedProcessorOptions = changeFeedProcessorOptions;
            this.leaseManager = leaseManager;
            this.collectionSelfLink = collectionSelfLink;
        }

        /// <summary>
        /// Creates a partition reader
        /// </summary>
        /// <param name="lease">Lease</param>
        /// <returns>A new <see cref="IPartitionReader"/></returns>
        public IPartitionReader Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            var renewer = new LeaseRenewer(lease, this.leaseManager, this.changeFeedProcessorOptions.LeaseRenewInterval);

            var settings = new ProcessorSettings
            {
                CollectionSelfLink = this.collectionSelfLink,
                RequestContinuation = !string.IsNullOrEmpty(lease.ContinuationToken) ?
                   lease.ContinuationToken :
                   this.changeFeedProcessorOptions.RequestContinuation,
                PartitionKeyRangeId = lease.PartitionId,
                FeedPollDelay = this.changeFeedProcessorOptions.FeedPollDelay,
                MaxItemCount = this.changeFeedProcessorOptions.MaxItemCount,
                StartFromBeginning = this.changeFeedProcessorOptions.StartFromBeginning,
                StartTime = this.changeFeedProcessorOptions.StartTime,
                SessionToken = this.changeFeedProcessorOptions.SessionToken,
            };

            return new PartitionReader(lease, renewer, settings, this.feedDocumentClient, new PartitionCheckpointer(this.leaseManager, lease));
        }
    }
}
