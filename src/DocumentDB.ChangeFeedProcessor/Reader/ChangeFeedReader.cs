//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class ChangeFeedReader : IChangeFeedReader
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IPartitionReaderManager partitionManager;

        public ChangeFeedReader(IPartitionReaderManager partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));
            this.partitionManager = partitionManager;
        }

        public async Task StartAsync()
        {
            Logger.InfoFormat("Starting processor...");
            await this.partitionManager.StartAsync().ConfigureAwait(false);
            Logger.InfoFormat("Processor started.");
        }

        public async Task StopAsync()
        {
            Logger.InfoFormat("Stopping processor...");
            await this.partitionManager.StopAsync().ConfigureAwait(false);
            Logger.InfoFormat("Processor stopped.");
        }

        public async Task<ChangeFeedDocumentChanges> ReadAsync()
        {
            return await this.partitionManager.ReadAsync().ConfigureAwait(false);
        }
    }
}