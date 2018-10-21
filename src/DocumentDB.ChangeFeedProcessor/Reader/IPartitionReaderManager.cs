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

    internal interface IPartitionReaderManager : IPartitionManager
    {
        Task<ChangeFeedDocumentChanges> ReadAsync();
    }
}
