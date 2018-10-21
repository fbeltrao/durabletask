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

    /// <summary>
    /// Partition reader
    /// </summary>
    public interface IChangeFeedReader
    {
        /// <summary>
        /// Start processing
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task StartAsync();

        /// <summary>
        /// Stops listening for changes.
        /// </summary>
        /// <returns>A <see cref="Task"/>.</returns>
        Task StopAsync();

        /// <summary>
        /// Reads a partition
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task<ChangeFeedDocumentChanges> ReadAsync();
    }
}
