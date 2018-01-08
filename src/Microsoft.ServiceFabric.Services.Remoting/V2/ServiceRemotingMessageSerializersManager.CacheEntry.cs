// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V2
{
    internal class CacheEntry
    {
        public CacheEntry(
            IServiceRemotingRequestMessageBodySerializer requestBodySerializer,
            IServiceRemotingResponseMessageBodySerializer responseBodySerializer)
        {
            this.RequestBodySerializer = requestBodySerializer;
            this.ResponseBodySerializer = responseBodySerializer;
        }

        public IServiceRemotingRequestMessageBodySerializer RequestBodySerializer { get; }

        public IServiceRemotingResponseMessageBodySerializer ResponseBodySerializer { get; }
    }
}