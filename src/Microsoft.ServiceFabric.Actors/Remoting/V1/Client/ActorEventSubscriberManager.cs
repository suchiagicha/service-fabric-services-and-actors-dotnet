// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Remoting.V1.Client
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Actors.Client;
    using Microsoft.ServiceFabric.Actors.Remoting.V1.Builder;
    using Microsoft.ServiceFabric.Services.Common;
    using Microsoft.ServiceFabric.Services.Remoting.V1;

    internal class ActorEventSubscriberManager : IServiceRemotingCallbackClient
    {
        public static readonly ActorEventSubscriberManager Singleton = new ActorEventSubscriberManager();

        private readonly ConcurrentDictionary<Subscriber, SubscriptionInfo> eventKeyToInfoMap;
        private readonly ConcurrentDictionary<Guid, SubscriptionInfo> subscriptionIdToInfoMap;
        private readonly ConcurrentDictionary<int, ActorMethodDispatcherBase> eventIdToDispatchersMap;

        private ActorEventSubscriberManager()
        {
            this.eventIdToDispatchersMap = new ConcurrentDictionary<int, ActorMethodDispatcherBase>();
            this.eventKeyToInfoMap = new ConcurrentDictionary<Subscriber, SubscriptionInfo>();
            this.subscriptionIdToInfoMap = new ConcurrentDictionary<Guid, SubscriptionInfo>();
        }

        public Task<byte[]> RequestResponseAsync(ServiceRemotingMessageHeaders messageHeaders, byte[] requestBody)
        {
            throw new NotImplementedException();
        }

        public void OneWayMessage(ServiceRemotingMessageHeaders serviceMessageHeaders, byte[] requestBody)
        {
            ActorMessageHeaders actorHeaders;
            if (!ActorMessageHeaders.TryFromServiceMessageHeaders(serviceMessageHeaders, out actorHeaders))
            {
                return;
            }

            ActorMethodDispatcherBase eventDispatcher;
            if (this.eventIdToDispatchersMap == null ||
                !this.eventIdToDispatchersMap.TryGetValue(actorHeaders.InterfaceId, out eventDispatcher))
            {
                return;
            }

            SubscriptionInfo info;
            if (!this.subscriptionIdToInfoMap.TryGetValue(actorHeaders.ActorId.GetGuidId(), out info))
            {
                return;
            }

            if (info.Subscriber.EventId != actorHeaders.InterfaceId)
            {
                return;
            }

            try
            {
                object eventMsgBody = eventDispatcher.DeserializeRequestMessageBody(requestBody);
                eventDispatcher.Dispatch(info.Subscriber.Instance, actorHeaders.MethodId, eventMsgBody);
            }
            catch
            {
                // ignored
            }
        }

        public void RegisterEventDispatchers(IEnumerable<ActorMethodDispatcherBase> eventDispatchers)
        {
            if (eventDispatchers != null)
            {
                foreach (ActorMethodDispatcherBase dispatcher in eventDispatchers)
                {
                    this.eventIdToDispatchersMap.GetOrAdd(
                        dispatcher.InterfaceId,
                        dispatcher);
                }
            }
        }

        public SubscriptionInfo RegisterSubscriber(ActorId actorId, Type eventInterfaceType, object instance)
        {
            int eventId = this.GetAndEnsureEventId(eventInterfaceType);

            var key = new Subscriber(actorId, eventId, instance);
            SubscriptionInfo info = this.eventKeyToInfoMap.GetOrAdd(key, k => new SubscriptionInfo(k));
            this.subscriptionIdToInfoMap.GetOrAdd(info.Id, i => info);

            return info;
        }

        public bool TryUnregisterSubscriber(ActorId actorId, Type eventInterfaceType, object instance, out SubscriptionInfo info)
        {
            int eventId = this.GetAndEnsureEventId(eventInterfaceType);

            var key = new Subscriber(actorId, eventId, instance);
            if (this.eventKeyToInfoMap.TryRemove(key, out info))
            {
                info.IsActive = false;

                SubscriptionInfo info2;
                this.subscriptionIdToInfoMap.TryRemove(info.Id, out info2);
                return true;
            }

            return false;
        }

        private int GetAndEnsureEventId(Type eventInterfaceType)
        {
            if (this.eventIdToDispatchersMap != null)
            {
                int eventId = IdUtil.ComputeId(eventInterfaceType);
                if (this.eventIdToDispatchersMap.ContainsKey(eventId))
                {
                    return eventId;
                }
            }

            throw new ArgumentException();
        }
    }
}