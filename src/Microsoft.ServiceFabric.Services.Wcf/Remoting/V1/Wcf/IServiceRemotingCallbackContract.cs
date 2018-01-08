﻿// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V1.Wcf
{
    using System.ServiceModel;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Services.Communication.Wcf;

    /// <summary>
    ///     Defines the interface that must be implemented for providing callback mechanism
    ///     from the wcf remoting listener to the client.
    /// </summary>
    [ServiceContract(Namespace = WcfConstants.Namespace)]
    public interface IServiceRemotingCallbackContract
    {
        /// <summary>
        ///     Sends a message to the client and gets the response.
        /// </summary>
        /// <param name="messageHeaders">
        ///     Message Headers contains the information needed to deserialize request and to dispatch
        ///     message to the client.
        /// </param>
        /// <param name="requestBody"> Message Body contains a request in a serialized form.</param>
        /// <returns>Response Body is a serialized response received by the service.</returns>
#pragma warning disable 108
        [OperationContract]
        [FaultContract(typeof(RemoteExceptionInformation))]
        Task<byte[]> RequestResponseAsync(ServiceRemotingMessageHeaders messageHeaders, byte[] requestBody);

        /// <summary>
        ///     Sends a one way message to the client.
        /// </summary>
        /// <param name="messageHeaders">
        ///     Message Headers contains the information needed to deserialize request and to dispatch
        ///     message to the client.
        /// </param>
        /// <param name="requestBody"> Message Body contains a request in a serialized form.</param>
        [OperationContract(IsOneWay = true)]
        void SendOneWay(ServiceRemotingMessageHeaders messageHeaders, byte[] requestBody);
    }
}