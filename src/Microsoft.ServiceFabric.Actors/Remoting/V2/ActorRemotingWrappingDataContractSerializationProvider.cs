// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Actors.Remoting.V2
{
    using System.Runtime.Serialization;
    using Microsoft.ServiceFabric.Services.Remoting.V2;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;
    /// <summary>
    /// TODO : add documentation
    /// </summary>
    public class ActorRemotingWrappingDataContractSerializationProvider : WrappingServiceRemotingDataContractSerializationProvider
    {

        /// <summary>
        ///     Creates an ActorRemotingWrappingDataContractSerializationProvider with default IBufferPoolManager
        /// </summary>
        public ActorRemotingWrappingDataContractSerializationProvider()
        {
        }

        /// <summary>
        ///     Creates an ActorRemotingWrappingDataContractSerializationProvider with user specified IBufferPoolManager.
        ///     If the specified buffer pool manager is null, the buffer pooling will be turned off.
        /// </summary>
        /// <param name="bodyBufferPoolManager"></param>
        public ActorRemotingWrappingDataContractSerializationProvider(
            IBufferPoolManager bodyBufferPoolManager)
            : base(bodyBufferPoolManager)
        {
        }

        /// <inheritdoc />
        protected  override DataContractSerializer CreateRemotingRequestMessageBodyDataContractSerializer(
            Type remotingRequestType,
            IEnumerable<Type> knownTypes)
        {

#if DotNetCoreClr
            var serializer = base.CreateRemotingRequestMessageBodyDataContractSerializer(remotingRequestType, knownTypes);
              
            serializer.SetSerializationSurrogateProvider(new ActorDataContractSurrogate());
            return serializer;

#else
            return new DataContractSerializer
                (remotingRequestType,
                new DataContractSerializerSettings
                {
                    MaxItemsInObjectGraph = int.MaxValue,
                    KnownTypes = knownTypes,
                   DataContractSurrogate = new ActorDataContractSurrogate()
                });
#endif

        }

        /// <inheritdoc />
        protected  override DataContractSerializer CreateRemotingResponseMessageBodyDataContractSerializer(
            Type remotingResponseType,
            IEnumerable<Type> knownTypes)
        {
#if DotNetCoreClr
            var serializer = base.CreateRemotingResponseMessageBodyDataContractSerializer(remotingResponseType, knownTypes);
              
            serializer.SetSerializationSurrogateProvider(new ActorDataContractSurrogate());
            return serializer;

#else
            return new DataContractSerializer
                (remotingResponseType,
                new DataContractSerializerSettings
                {
                    MaxItemsInObjectGraph = int.MaxValue,
                    KnownTypes = knownTypes,
                   DataContractSurrogate = new ActorDataContractSurrogate()
                });
#endif

        }
    }
}
