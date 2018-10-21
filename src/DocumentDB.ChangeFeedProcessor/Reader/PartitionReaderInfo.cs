//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    internal class PartitionReaderInfo
    {
        internal TaskCompletionSource<bool> TaskCompletionSource { get; set; }

        internal IPartitionReader Reader { get; set; }
    }
}
