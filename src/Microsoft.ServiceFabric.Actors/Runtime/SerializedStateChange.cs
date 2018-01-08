// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    internal class SerializedStateChange
    {
        public SerializedStateChange(StateChangeKind changeKind, string key, byte[] serializedState)
        {
            this.ChangeKind = changeKind;
            this.Key = key;
            this.SerializedState = serializedState;
        }

        public StateChangeKind ChangeKind { get; }

        public string Key { get; }

        public byte[] SerializedState { get; }
    }
}