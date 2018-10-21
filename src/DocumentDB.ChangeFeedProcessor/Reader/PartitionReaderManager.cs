//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class PartitionReaderManager : IPartitionReaderManager
    {
        private readonly IBootstrapper bootstrapper;
        private readonly IPartitionReaderController partitionController;
        private readonly IPartitionLoadBalancer partitionLoadBalancer;

        public PartitionReaderManager(IBootstrapper bootstrapper, IPartitionReaderController partitionController, IPartitionLoadBalancer partitionLoadBalancer)
        {
            this.bootstrapper = bootstrapper;
            this.partitionController = partitionController;
            this.partitionLoadBalancer = partitionLoadBalancer;
        }

        public async Task<ChangeFeedDocumentChanges> ReadAsync()
        {
            return await this.partitionController.ReadAsync().ConfigureAwait(false);
        }

        public async Task StartAsync()
        {
            await this.bootstrapper.InitializeAsync().ConfigureAwait(false);
            await this.partitionController.InitializeAsync().ConfigureAwait(false);
            this.partitionLoadBalancer.Start();
        }

        public async Task StopAsync()
        {
            await this.partitionLoadBalancer.StopAsync().ConfigureAwait(false);
            await this.partitionController.ShutdownAsync().ConfigureAwait(false);
        }
    }
}