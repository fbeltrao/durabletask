//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Partition reader
    /// </summary>
    internal class PartitionReader : IPartitionReader
    {
        private static readonly int DefaultMaxItemCount = 100;
        private readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly ILease lease;
        private readonly LeaseRenewer renewer;
        private readonly ProcessorSettings settings;
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly IPartitionCheckpointer checkpointer;
        private readonly ChangeFeedOptions options;
        private readonly CancellationTokenSource renewerCancellation = new CancellationTokenSource();
        private string lastContinuation;
        private IChangeFeedDocumentQuery<Document> query;

        public PartitionReader(ILease lease, LeaseRenewer renewer, ProcessorSettings settings, IChangeFeedDocumentClient documentClient, IPartitionCheckpointer checkpointer)
        {
            this.lease = lease;
            this.renewer = renewer;
            this.settings = settings;
            this.documentClient = documentClient;
            this.checkpointer = checkpointer;
            this.options = new ChangeFeedOptions
            {
                MaxItemCount = settings.MaxItemCount,
                PartitionKeyRangeId = settings.PartitionKeyRangeId,
                SessionToken = settings.SessionToken,
                StartFromBeginning = settings.StartFromBeginning,
                RequestContinuation = settings.RequestContinuation,
                StartTime = settings.StartTime,
            };
        }

        public Task StartAsync(CancellationToken shutdownToken)
        {
            Task renewerTask = this.renewer.RunAsync(this.renewerCancellation.Token);
            renewerTask.LogException();

            return Task.FromResult(0);

            ////ChangeFeedObserverCloseReason closeReason = shutdownToken.IsCancellationRequested ?
            ////    ChangeFeedObserverCloseReason.Shutdown :
            ////    ChangeFeedObserverCloseReason.Unknown;

            ////try
            ////{
            ////    await renewerTask.ConfigureAwait(false);
            ////}
            ////catch (LeaseLostException)
            ////{
            ////    closeReason = ChangeFeedObserverCloseReason.LeaseLost;
            ////    throw;
            ////}
            ////catch (PartitionSplitException)
            ////{
            ////    closeReason = ChangeFeedObserverCloseReason.LeaseGone;
            ////    throw;
            ////}
            ////catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            ////{
            ////    closeReason = ChangeFeedObserverCloseReason.Shutdown;
            ////}
        }

        public void Dispose()
        {
            this.renewerCancellation.Dispose();
        }

        public async Task<ChangeFeedDocumentChanges> ReadAsync()
        {
            this.lastContinuation = this.lastContinuation ?? this.settings.RequestContinuation;

            if (this.query == null)
            {
                this.query = this.documentClient.CreateDocumentChangeFeedQuery(this.settings.CollectionSelfLink, this.options);
            }

            // TODO: fix cts
            CancellationToken cancellationToken = CancellationToken.None;

            try
            {
                IFeedResponse<Document> response = null;

                // TODO: review this number
                var docs = new List<Document>(this.options.MaxItemCount ?? 10);

                do
                {
                    response = await this.query.ExecuteNextAsync<Document>(cancellationToken).ConfigureAwait(false);
                    this.lastContinuation = response.ResponseContinuation;
                    if (response.Count > 0)
                    {
                        using (IEnumerator<Document> e = response.GetEnumerator())
                        {
                            while (e.MoveNext())
                            {
                                docs.Add(e.Current);
                            }
                        }
                    }
                }
                while (this.query.HasMoreResults && docs.Count < this.options.MaxItemCount && !cancellationToken.IsCancellationRequested);

                IChangeFeedObserverContext context = new ChangeFeedObserverContext(this.settings.PartitionKeyRangeId, response, this.checkpointer);
                return new ChangeFeedDocumentChanges(docs, context);
            }
            catch (DocumentClientException clientException)
            {
                this.logger.WarnException("exception: partition '{0}'", clientException, this.settings.PartitionKeyRangeId);
                DocDbError docDbError = ExceptionClassifier.ClassifyClientException(clientException);
                switch (docDbError)
                {
                    case DocDbError.PartitionNotFound:
                        throw new PartitionNotFoundException("Partition not found.", this.lastContinuation);
                    case DocDbError.PartitionSplit:
                        throw new PartitionSplitException("Partition split.", this.lastContinuation);
                    case DocDbError.Undefined:
                        throw;
                    case DocDbError.MaxItemCountTooLarge:
                        if (!this.options.MaxItemCount.HasValue)
                        {
                            this.options.MaxItemCount = DefaultMaxItemCount;
                        }
                        else if (this.options.MaxItemCount <= 1)
                        {
                            this.logger.ErrorFormat("Cannot reduce maxItemCount further as it's already at {0}.", this.options.MaxItemCount);
                            throw;
                        }

                        this.options.MaxItemCount /= 2;
                        this.logger.WarnFormat("Reducing maxItemCount, new value: {0}.", this.options.MaxItemCount);
                        break;
                }
            }
            catch (TaskCanceledException canceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw;

                this.logger.WarnException("exception: partition '{0}'", canceledException, this.settings.PartitionKeyRangeId);

                // ignore as it is caused by DocumentDB client
            }

            return new ChangeFeedDocumentChanges();
        }
    }
}
