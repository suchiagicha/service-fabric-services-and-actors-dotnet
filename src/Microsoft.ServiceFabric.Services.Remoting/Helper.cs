// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Remoting
{
    class Helper
    {
        public static bool IsEitherRemotingV2(RemotingClientVersion remotingClient)
        {
            return IsRemotingV2(remotingClient) || IsRemotingV2InterfaceCompatibleVersion(remotingClient);
        }

        public static bool IsEitherRemotingV2(RemotingListenerVersion remotingListener)
        {
            return IsRemotingV2(remotingListener) || IsRemotingV2InterfaceCompatibleVersion(remotingListener);
        }

        public static bool IsRemotingV2(RemotingClientVersion remotingClient)
        {
            return remotingClient.HasFlag(RemotingClientVersion.V2);
        }
        public static bool IsRemotingV2(RemotingListenerVersion remotingListener)
        {
            return remotingListener.HasFlag(RemotingListenerVersion.V2);
        }

        public static bool IsRemotingV2InterfaceCompatibleVersion(RemotingListenerVersion remotingListener)
        {
            return remotingListener.HasFlag(RemotingListenerVersion.V2InterfaceCompatible);
        }

        public static bool IsRemotingV2InterfaceCompatibleVersion(RemotingClientVersion remotingListener)
        {
            return remotingListener.HasFlag(RemotingClientVersion.V2InterfaceCompatible);
        }

#if !DotNetCoreClr
        public static bool IsRemotingV1(RemotingListenerVersion remotingListener)
        {
            return remotingListener.HasFlag(RemotingListenerVersion.V1);
        }

          public static bool IsRemotingV1(RemotingClientVersion remotingListener)
        {
            return remotingListener.HasFlag(RemotingClientVersion.V1);
        }
#endif


    }
}
