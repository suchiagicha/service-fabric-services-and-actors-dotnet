// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Client
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.FabricTransport.V2;
    using Microsoft.ServiceFabric.FabricTransport.V2.Client;
    using Microsoft.ServiceFabric.Services.Communication;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Client;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;
    using SR = Microsoft.ServiceFabric.Services.Remoting.SR;

    internal class FabricTransportServiceRemotingClient : IServiceRemotingClient
    {
        private readonly ServiceRemotingMessageSerializersManager serializersManager;

        private readonly FabricTransportClient fabricTransportClient;
        // we need to pass a cache of the serializers here rather than the known types, 
        // the serializer cache should be maintained by the factor

        internal FabricTransportServiceRemotingClient(
            ServiceRemotingMessageSerializersManager serializersManager,
            FabricTransportClient fabricTransportClient)
        {
            this.fabricTransportClient = fabricTransportClient;
            this.serializersManager = serializersManager;
            this.IsValid = true;
        }

        public bool IsValid { get; private set; }

        public object ConnectionAddress
        {
            get { return this.fabricTransportClient.ConnectionAddress; }
        }

        public ResolvedServicePartition ResolvedServicePartition { get; set; }

        public string ListenerName { get; set; }

        public ResolvedServiceEndpoint Endpoint { get; set; }

        public async Task<IServiceRemotingResponseMessage> RequestResponseAsync(
            IServiceRemotingRequestMessage remotingRequestRequestMessage)
        {
            int interfaceId = remotingRequestRequestMessage.GetHeader().InterfaceId;
            IMessageHeader serializedHeader = this.serializersManager.GetHeaderSerializer()
                .SerializeRequestHeader(remotingRequestRequestMessage.GetHeader());
            IServiceRemotingRequestMessageBodySerializer msgBodySeriaizer = this.serializersManager.GetRequestBodySerializer(interfaceId);
            OutgoingMessageBody serializedMsgBody = msgBodySeriaizer.Serialize(remotingRequestRequestMessage.GetBody());
            FabricTransportRequestBody fabricTransportRequestBody = serializedMsgBody != null
                ? new FabricTransportRequestBody(
                    serializedMsgBody.GetSendBuffers(),
                    serializedMsgBody.Dispose)
                : new FabricTransportRequestBody(new List<ArraySegment<byte>>(), null);


            //Send Request
            using (FabricTransportMessage retval = await this.fabricTransportClient.RequestResponseAsync(
                new FabricTransportMessage(
                    new FabricTransportRequestHeader(serializedHeader.GetSendBuffer(), serializedHeader.Dispose),
                    fabricTransportRequestBody),
                this.fabricTransportClient.Settings.OperationTimeout))
            {
                IncomingMessageHeader incomingHeader = retval != null && retval.GetHeader() != null
                    ? new IncomingMessageHeader(retval.GetHeader().GetRecievedStream())
                    : null;

                ////DeSerialize Response
                IServiceRemotingResponseMessageHeader header =
                    this.serializersManager.GetHeaderSerializer()
                        .DeserializeResponseHeaders(
                            incomingHeader);

                byte[] headerValue;
                if (header != null && header.TryGetHeaderValue("HasRemoteException", out headerValue))
                {
                    Exception e;
                    bool isDeserialzied =
                        RemoteException.ToException(
                            retval.GetBody().GetRecievedStream(),
                            out e);
                    if (isDeserialzied)
                    {
                        throw new AggregateException(e);
                    }

                    throw new ServiceException(
                        e.GetType().FullName,
                        string.Format(
                            CultureInfo.InvariantCulture,
                            SR.ErrorDeserializationFailure,
                            e.ToString()));
                }

                IServiceRemotingResponseMessageBodySerializer responseSerializer = this.serializersManager.GetResponseBodySerializer(interfaceId);
                IServiceRemotingResponseMessageBody responseMessageBody = null;
                if (retval != null && retval.GetBody() != null)
                {
                    responseMessageBody =
                        responseSerializer.Deserialize(new IncomingMessageBody(retval.GetBody().GetRecievedStream()));
                }

                return new ServiceRemotingResponseMessage(header, responseMessageBody);
            }
        }

        public void SendOneWay(IServiceRemotingRequestMessage requestMessage)
        {
            throw new NotImplementedException();
        }

        public Task OpenAsync(CancellationToken cancellationToken)
        {
            return this.fabricTransportClient.OpenAsync(cancellationToken);
        }

        public void Abort()
        {
            this.IsValid = false;
            this.fabricTransportClient.Abort();
        }

        ~FabricTransportServiceRemotingClient()
        {
            if (this.fabricTransportClient != null)
            {
                this.fabricTransportClient.Dispose();
            }
        }
    }
}