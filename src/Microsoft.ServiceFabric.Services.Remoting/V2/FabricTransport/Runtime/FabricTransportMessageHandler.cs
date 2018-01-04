﻿// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.FabricTransport.V2;
    using Microsoft.ServiceFabric.FabricTransport.V2.Runtime;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Diagnostic;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Runtime;

    internal class FabricTransportMessageHandler : IFabricTransportMessageHandler
    {
        private readonly IServiceRemotingMessageHandler remotingMessageHandler;
        private readonly ServiceRemotingMessageSerializersManager serializersManager;
        private readonly Guid partitionId;
        private readonly long replicaOrInstanceId;
        private readonly ServiceRemotingPerformanceCounterProvider serviceRemotingPerformanceCounterProvider;
        private readonly IServiceRemotingMessageHeaderSerializer headerSerializer;

        public FabricTransportMessageHandler(
            IServiceRemotingMessageHandler remotingMessageHandler,
            ServiceRemotingMessageSerializersManager serializersManager,
            Guid partitionId,
            long replicaOrInstanceId)
        {
            this.remotingMessageHandler = remotingMessageHandler;
            this.serializersManager = serializersManager;
            this.partitionId = partitionId;
            this.replicaOrInstanceId = replicaOrInstanceId;
            this.serviceRemotingPerformanceCounterProvider = new ServiceRemotingPerformanceCounterProvider(
                this.partitionId,
                this.replicaOrInstanceId);
            this.headerSerializer = this.serializersManager.GetHeaderSerializer();
        }

        public async Task<FabricTransportMessage> RequestResponseAsync(
            FabricTransportRequestContext requestContext,
            FabricTransportMessage fabricTransportMessage)
        {
            if (null != this.serviceRemotingPerformanceCounterProvider.serviceOutstandingRequestsCounterWriter)
            {
                this.serviceRemotingPerformanceCounterProvider.serviceOutstandingRequestsCounterWriter
                    .UpdateCounterValue(1);
            }

            Stopwatch requestStopWatch = Stopwatch.StartNew();
            Stopwatch requestResponseSerializationStopwatch = Stopwatch.StartNew();

            try
            {
                IServiceRemotingRequestMessage remotingRequestMessage = this.CreateRemotingRequestMessage(
                    fabricTransportMessage,
                    requestResponseSerializationStopwatch
                );

                IServiceRemotingResponseMessage retval = await
                    this.remotingMessageHandler.HandleRequestResponseAsync(
                        new FabricTransportServiceRemotingRequestContext(requestContext, this.serializersManager),
                        remotingRequestMessage);
                return this.CreateFabricTransportMessage(retval, remotingRequestMessage.GetHeader().InterfaceId, requestResponseSerializationStopwatch);
            }
            catch (Exception ex)
            {
                ServiceTrace.Source.WriteInfo("FabricTransportMessageHandler", "Remote Exception occured {0}", ex);
                return this.CreateFabricTransportExceptionMessage(ex);
            }
            finally
            {
                fabricTransportMessage.Dispose();
                if (null != this.serviceRemotingPerformanceCounterProvider.serviceOutstandingRequestsCounterWriter)
                {
                    this.serviceRemotingPerformanceCounterProvider.serviceOutstandingRequestsCounterWriter
                        .UpdateCounterValue(-1);
                }

                if (null != this.serviceRemotingPerformanceCounterProvider.serviceRequestProcessingTimeCounterWriter)
                {
                    this.serviceRemotingPerformanceCounterProvider.serviceRequestProcessingTimeCounterWriter
                        .UpdateCounterValue(
                            requestStopWatch.ElapsedMilliseconds);
                }
            }
        }

        public void HandleOneWay(
            FabricTransportRequestContext requestContext,
            FabricTransportMessage requesTransportMessage)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            if (this.serviceRemotingPerformanceCounterProvider != null)
            {
                this.serviceRemotingPerformanceCounterProvider.Dispose();
            }

            var disposableItem = this.remotingMessageHandler as IDisposable;
            if (null != disposableItem)
            {
                disposableItem.Dispose();
            }
        }

        private FabricTransportMessage CreateFabricTransportExceptionMessage(Exception ex)
        {
            var header = new ServiceRemotingResponseMessageHeader();
            header.AddHeader("HasRemoteException", new byte[0]);
            IMessageHeader serializedHeader = this.serializersManager.GetHeaderSerializer().SerializeResponseHeader(header);
            RemoteException serializedMsg = RemoteException.FromException(ex);
            var msg = new FabricTransportMessage(
                new FabricTransportRequestHeader(serializedHeader.GetSendBuffer(), serializedHeader.Dispose),
                new FabricTransportRequestBody(serializedMsg.Data, null));
            return msg;
        }

        private FabricTransportMessage CreateFabricTransportMessage(IServiceRemotingResponseMessage retval, int interfaceId, Stopwatch stopwatch)
        {
            if (retval == null)
            {
                return new FabricTransportMessage(null, null);
            }

            IMessageHeader responseHeader = this.headerSerializer.SerializeResponseHeader(retval.GetHeader());
            FabricTransportRequestHeader fabricTransportRequestHeader = responseHeader != null
                ? new FabricTransportRequestHeader(
                    responseHeader.GetSendBuffer(),
                    responseHeader.Dispose)
                : new FabricTransportRequestHeader(new ArraySegment<byte>(), null);
            IServiceRemotingResponseMessageBodySerializer responseSerializer =
                this.serializersManager.GetResponseBodySerializer(interfaceId);
            stopwatch.Restart();
            OutgoingMessageBody responseMsgBody = responseSerializer.Serialize(retval.GetBody());
            if (this.serviceRemotingPerformanceCounterProvider.serviceResponseSerializationTimeCounterWriter != null)
            {
                this.serviceRemotingPerformanceCounterProvider.serviceResponseSerializationTimeCounterWriter
                    .UpdateCounterValue(stopwatch.ElapsedMilliseconds);
            }

            FabricTransportRequestBody fabricTransportRequestBody = responseMsgBody != null
                ? new FabricTransportRequestBody(
                    responseMsgBody.GetSendBuffers(),
                    responseMsgBody.Dispose)
                : new FabricTransportRequestBody(new List<ArraySegment<byte>>(), null);

            var message = new FabricTransportMessage(
                fabricTransportRequestHeader,
                fabricTransportRequestBody);
            return message;
        }

        private IServiceRemotingRequestMessage CreateRemotingRequestMessage(
            FabricTransportMessage fabricTransportMessage, Stopwatch stopwatch)
        {
            IServiceRemotingRequestMessageHeader deSerializedHeader = this.headerSerializer.DeserializeRequestHeaders(
                new IncomingMessageHeader(fabricTransportMessage.GetHeader().GetRecievedStream()));
            IServiceRemotingRequestMessageBodySerializer msgBodySerializer =
                this.serializersManager.GetRequestBodySerializer(deSerializedHeader.InterfaceId);
            stopwatch.Restart();
            IServiceRemotingRequestMessageBody deserializedMsg = msgBodySerializer.Deserialize(
                new IncomingMessageBody(fabricTransportMessage.GetBody().GetRecievedStream()));
            if (this.serviceRemotingPerformanceCounterProvider.serviceRequestDeserializationTimeCounterWriter != null)
            {
                this.serviceRemotingPerformanceCounterProvider.serviceRequestDeserializationTimeCounterWriter.UpdateCounterValue
                (
                    stopwatch.ElapsedMilliseconds);
            }

            return new ServiceRemotingRequestMessage(deSerializedHeader, deserializedMsg);
        }
    }
}