// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    using System;
    using Microsoft.ServiceFabric.Actors.Remoting.V1;
    using Microsoft.ServiceFabric.Actors.Remoting.V2;
    using Microsoft.ServiceFabric.Services.Remoting;
    using Microsoft.ServiceFabric.Services.Remoting.V1;
    using Microsoft.ServiceFabric.Services.Remoting.V2;

    internal class ActorEventSubscriberProxy : IActorEventSubscriberProxy
    {
#if !DotNetCoreClr
        private readonly IServiceRemotingCallbackClient callback;
#endif
        private readonly Services.Remoting.V2.Runtime.IServiceRemotingCallbackClient callbackV2;
        private readonly Guid id;

#if !DotNetCoreClr
        public ActorEventSubscriberProxy(Guid id, IServiceRemotingCallbackClient callback)
        {
            this.id = id;
            this.callback = callback;
            this.RemotingListener = RemotingListener.V1Listener;
        }

#endif
        public ActorEventSubscriberProxy(Guid id, Services.Remoting.V2.Runtime.IServiceRemotingCallbackClient callback)
        {
            this.id = id;
            this.callbackV2 = callback;
            this.RemotingListener = RemotingListener.V2Listener;
        }

        Guid IActorEventSubscriberProxy.Id
        {
            get { return this.id; }
        }

        public RemotingListener RemotingListener { get; }

#if !DotNetCoreClr
        void IActorEventSubscriberProxy.RaiseEvent(int eventInterfaceId, int eventMethodId, byte[] eventMsgBody)
        {
            this.callback.OneWayMessage(
                new ActorMessageHeaders
                {
                    ActorId = new ActorId(this.id),
                    InterfaceId = eventInterfaceId,
                    MethodId = eventMethodId
                }.ToServiceMessageHeaders(),
                eventMsgBody);
        }
#endif

        public void RaiseEvent(int eventInterfaceId, int methodId, IServiceRemotingRequestMessageBody eventMsgBody)
        {
            var headers = new ActorRemotingMessageHeaders
            {
                ActorId = new ActorId(this.id),
                InterfaceId = eventInterfaceId,
                MethodId = methodId
            };

            this.callbackV2.SendOneWay(
                new ServiceRemotingRequestMessage(
                    headers,
                    eventMsgBody));
        }

        public IServiceRemotingMessageBodyFactory GetRemotingMessageBodyFactory()
        {
            if (this.RemotingListener.Equals(RemotingListener.V2Listener))
            {
                return this.callbackV2.GetRemotingMessageBodyFactory();
            }

            throw new NotSupportedException("MessageFactory is not supported for V1Listener");
        }
    }
}