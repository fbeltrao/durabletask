//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// Builder for <see cref="IPartitionReader"/>
    /// </summary>
    public interface IPartitionReaderFactory
    {
        /// <summary>
        /// Creates a partition reader
        /// </summary>
        /// <param name="lease">Lease</param>
        /// <returns>A new <see cref="IPartitionReader"/></returns>
        IPartitionReader Create(ILease lease);
    }
}
