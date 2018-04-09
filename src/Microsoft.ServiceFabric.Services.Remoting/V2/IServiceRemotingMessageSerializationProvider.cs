// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V2
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Defines the interface that must be implemented for providing custom serialization for the remoting request.
    /// </summary>
    public interface IServiceRemotingMessageSerializationProvider
    {
        /// <summary>
        /// Create a IServiceRemotingMessageBodyFactory used for creating remoting request and response body.
        /// </summary>
        /// <returns></returns>
        IServiceRemotingMessageBodyFactory CreateMessageBodyFactory();

        
        /// <summary>
        /// Creates IServiceRemotingRequestMessageBodySerializer for a serviceInterface .
        /// </summary>
        /// <param name="serviceInterfaceType">User service interface</param>
        /// <param name="requestWrappedType"></param>
        /// <param name="requestBodyTypes">Parameters for all the methods in the serviceInterfaceType</param>
        /// <returns></returns>
        /// <returns></returns>
        IServiceRemotingRequestMessageBodySerializer CreateRequestMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> requestWrappedType,
            IEnumerable<Type> requestBodyTypes=null);

        /// <summary>
        ///  Creates IServiceRemotingResponseMessageBodySerializer for a serviceInterface .
        ///  </summary>
        ///  <param name="serviceInterfaceType">User service interface</param>
        /// <param name="responseWrappedType"></param>
        /// <param name="responseBodyTypes">Return Types for all the methods in the serviceInterfaceType</param>
        ///  <returns></returns>
        IServiceRemotingResponseMessageBodySerializer CreateResponseMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> responseWrappedType,
            IEnumerable<Type> responseBodyTypes=null);

    }
}
