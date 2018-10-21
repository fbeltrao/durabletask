//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    /// <summary>
    /// Result of a <see cref="IPartitionReader.ReadAsync"/> returning a collection of <see cref="Document"/>
    /// </summary>
    public class ChangeFeedDocumentChanges
    {
        private IReadOnlyList<IChangeFeedObserverContext> contexts;

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedDocumentChanges"/> class.
        /// </summary>
        public ChangeFeedDocumentChanges()
        {
            this.Docs = new Document[0];
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedDocumentChanges"/> class.
        /// </summary>
        /// <param name="context">Context</param>
        /// <param name="docs">Documents</param>
        public ChangeFeedDocumentChanges(IReadOnlyList<Document> docs, IChangeFeedObserverContext context)
        {
            this.Docs = docs;
            this.contexts = new IChangeFeedObserverContext[] { context };
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedDocumentChanges"/> class.
        /// </summary>
        /// <param name="contexts">Contexts</param>
        /// <param name="docs">Documents</param>
        public ChangeFeedDocumentChanges(IReadOnlyList<Document> docs, IReadOnlyList<IChangeFeedObserverContext> contexts)
        {
            this.Docs = docs;
            this.contexts = contexts;
        }

        /// <summary>
        /// Gets the documents that have changed
        /// </summary>
        public IReadOnlyList<Document> Docs { get; private set; }

        /// <summary>
        /// Saves the checkpoint where the change feed should resume reading.
        /// Does not affect the current reader as the cursor is kept in memory.
        /// Partition redistribution or host restart will resume from the updated checkpoint
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task SaveCheckpointAsync()
        {
            if (this.contexts != null)
            {
                var tasks = new List<Task>();
                foreach (var item in this.contexts)
                {
                    tasks.Add(item.CheckpointAsync());
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var logger = LogProvider.GetCurrentClassLogger();
                    logger.Error("Error saving checkpoint: " + ex.ToString());
                }
            }
        }

        internal static ChangeFeedDocumentChanges Combine(IEnumerable<ChangeFeedDocumentChanges> collection)
        {
            var docs = new List<Document>();
            var contexts = new List<IChangeFeedObserverContext>();
            foreach (var item in collection)
            {
                docs.AddRange(item.Docs);
                contexts.AddRange(item.contexts);
            }

            return new ChangeFeedDocumentChanges(docs, contexts);
        }
    }
}
