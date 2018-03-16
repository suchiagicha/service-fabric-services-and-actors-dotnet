// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Remoting.V2
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Microsoft.ServiceFabric.Services.Remoting.V2;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;

    /// <summary>
    ///     This is the default implmentation  for <see cref="IServiceRemotingMessageSerializationProvider" />used by actor
    ///     remoting.
    ///     It uses DataContractSerializer for serialization of remoting request and response message bodies.
    /// </summary>
    public class ActorRemotingDataContractSerializationProvider : ServiceRemotingDataContractSerializationProvider
    {
        /// <summary>
        ///     Creates an ActorRemotingDataContractSerializationProvider with default IBufferPoolManager
        /// </summary>
        public ActorRemotingDataContractSerializationProvider()
        {
        }

        /// <summary>
        ///     Creates an ActorRemotingDataContractSerializationProvider with user specified IBufferPoolManager.
        ///     If the specified buffer pool manager is null, the buffer pooling will be turned off.
        /// </summary>
        /// <param name="bodyBufferPoolManager"></param>
        public ActorRemotingDataContractSerializationProvider(
            IBufferPoolManager bodyBufferPoolManager)
            : base(bodyBufferPoolManager)
        {
        }

        /// <inheritdoc />
        protected override DataContractSerializer GetRemotingRequestMessageBodyDataContractSerializer(
            Type serviceInterfaceType,
            Type remotingRequestType,
            IEnumerable<Type> methodParameterTypes)
        {

#if DotNetCoreClr
            var serializer = base.GetRemotingRequestMessageBodyDataContractSerializer(serviceInterfaceType, remotingRequestType, methodParameterTypes);
            serializer.SetSerializationSurrogateProvider(new ActorDataContractSurrogate());
            return serializer;

#else
            return new DataContractSerializer
                (remotingRequestType,
                new DataContractSerializerSettings
                {
                    MaxItemsInObjectGraph = int.MaxValue,
                    KnownTypes = methodParameterTypes,
                   DataContractSurrogate = ActorDataContractSurrogate.Singleton
                });
#endif

        }

        /// <inheritdoc />
        protected override DataContractSerializer GetRemotingResponseMessageBodyDataContractSerializer(
            Type serviceInterfaceType,
            Type remotingResponseType,
            IEnumerable<Type> methodReturnTypes)
        {
#if DotNetCoreClr
            var serializer = base.GetRemotingRequestMessageBodyDataContractSerializer(serviceInterfaceType, remotingResponseType, methodReturnTypes);
            serializer.SetSerializationSurrogateProvider(new ActorDataContractSurrogate());
            return serializer;

#else
            return new DataContractSerializer
                (remotingResponseType,
                new DataContractSerializerSettings
                {
                    MaxItemsInObjectGraph = int.MaxValue,
                    KnownTypes = methodReturnTypes,
                   DataContractSurrogate = ActorDataContractSurrogate.Singleton
                });
#endif

        }
    }
}
