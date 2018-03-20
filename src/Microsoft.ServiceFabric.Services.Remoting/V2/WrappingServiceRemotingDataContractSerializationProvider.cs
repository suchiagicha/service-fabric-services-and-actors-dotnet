// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Remoting.V2;
using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;

namespace Microsoft.ServiceFabric.Services.Remoting.V2
{
    using System.IO;
    using System.Xml;

    /// <summary>
    /// TODO add doc
    /// </summary>
    public class WrappingServiceRemotingDataContractSerializationProvider : IServiceRemotingMessageSerializationProvider
    {
        private ServiceRemotingDataContractSerializationProvider internalprovider;
        /// <summary>
        /// Creates a ServiceRemotingDataContractSerializationProvider with default IBufferPoolManager 
        /// </summary>
        public WrappingServiceRemotingDataContractSerializationProvider()
        {
            this.internalprovider = new ServiceRemotingDataContractSerializationProvider();
        }

        /// <summary>
        /// Creates a ServiceRemotingDataContractSerializationProvider with user specified IBufferPoolManager
        /// </summary>
        /// <param name="bodyBufferPoolManager"></param>
        public WrappingServiceRemotingDataContractSerializationProvider(
            IBufferPoolManager bodyBufferPoolManager)
        {
            this.internalprovider = new ServiceRemotingDataContractSerializationProvider(bodyBufferPoolManager);
        }

        /// <summary>
        /// Create a IServiceRemotingMessageBodyFactory used for creating remoting request and response body.
        /// </summary>
        /// <returns></returns>
        public  IServiceRemotingMessageBodyFactory CreateMessageBodyFactory()
        {
            return new WrappedRequestMessageFactory();
        }

        /// <summary>
        /// Creates IServiceRemotingRequestMessageBodySerializer for a serviceInterface .
        /// </summary>
        /// <param name="serviceInterfaceType">User service interface</param>
        /// <param name="requestWrappedTypes"></param>
        /// <param name="requestBodyTypes">Parameters for all the methods in the serviceInterfaceType</param>
        /// <returns></returns>
        /// <returns></returns>
        public   IServiceRemotingRequestMessageBodySerializer CreateRequestMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> requestBodyTypes,
            IEnumerable<Type> requestWrappedTypes = null)
        {
            DataContractSerializer serializer = this.CreateRemotingRequestMessageBodyDataContractSerializer(
                typeof(WrappedRemotingMessageBody),
                requestWrappedTypes);

            return this.internalprovider.CreateRemotingRequestMessageSerializer<WrappedRemotingMessageBody, WrappedRemotingMessageBody>(
              serializer);
        }

        /// <summary>
        ///  Creates IServiceRemotingResponseMessageBodySerializer for a serviceInterface .
        ///  </summary>
        ///  <param name="serviceInterfaceType">User service interface</param>
        /// <param name="responseWrappedTypes"></param>
        /// <param name="responseBodyTypes">Return Types for all the methods in the serviceInterfaceType</param>
        public IServiceRemotingResponseMessageBodySerializer CreateResponseMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> responseBodyTypes,
            IEnumerable<Type> responseWrappedTypes = null)
        {
            DataContractSerializer serializer = this.CreateRemotingResponseMessageBodyDataContractSerializer(
                typeof(WrappedRemotingMessageBody),
                responseWrappedTypes);
            return this.internalprovider
                .CreateRemotingResponseMessageSerializer<WrappedRemotingMessageBody, WrappedRemotingMessageBody>(
                    serializer);
        }


        /// <summary>
        ///     Create the writer to write to the stream. Use this method to customize how the serialized contents are written to
        ///     the stream.
        /// </summary>
        /// <param name="outputStream">The stream on which to write the serialized contents.</param>
        /// <returns>
        ///     An <see cref="System.Xml.XmlDictionaryWriter" /> using which the serializer will write the object on the
        ///     stream.
        /// </returns>
        protected internal virtual XmlDictionaryWriter CreateXmlDictionaryWriter(Stream outputStream)
        {
            return this.internalprovider.CreateXmlDictionaryWriter(outputStream);
        }


        /// <summary>
        ///     Create the reader to read from the input stream. Use this method to customize how the serialized contents are read
        ///     from the stream.
        /// </summary>
        /// <param name="inputStream">The stream from which to read the serialized contents.</param>
        /// <returns>
        ///     An <see cref="System.Xml.XmlDictionaryReader" /> using which the serializer will read the object from the
        ///     stream.
        /// </returns>
        protected internal virtual XmlDictionaryReader CreateXmlDictionaryReader(Stream inputStream)
        {
            return this.internalprovider.CreateXmlDictionaryReader(inputStream);
        }

        /// <summary>
        ///     Gets the settings used to create DataContractSerializer for serializing and de-serializing request message body.
        /// </summary>
        /// <param name="remotingRequestType">Remoting RequestMessageBody Type</param>
        /// <param name="knownTypes">The return types of all of the methods of the specified interface.</param>
        /// <returns><see cref="DataContractSerializerSettings" /> for serializing and de-serializing request message body.</returns>
        protected virtual DataContractSerializer CreateRemotingRequestMessageBodyDataContractSerializer(
            Type remotingRequestType,
            IEnumerable<Type> knownTypes)
        {
            return this.internalprovider.CreateRemotingRequestMessageBodyDataContractSerializer(remotingRequestType,
                knownTypes);
        }

        /// <summary>
        ///     Gets the settings used to create DataContractSerializer for serializing and de-serializing request message body.
        /// </summary>
        /// <param name="remotingResponseType">Remoting ResponseMessage Type</param>
        /// <param name="knownTypes">The return types of all of the methods of the specified interface.</param>
        /// <returns><see cref="DataContractSerializerSettings" /> for serializing and de-serializing request message body.</returns>
        protected virtual DataContractSerializer CreateRemotingResponseMessageBodyDataContractSerializer(
            Type remotingResponseType,
            IEnumerable<Type> knownTypes)
        {
            return this.internalprovider.CreateRemotingResponseMessageBodyDataContractSerializer(remotingResponseType,
                knownTypes);
        }
    }
}

[DataContract(Name = "WrappedMsgBody", Namespace = Constants.ServiceCommunicationNamespace)]
class WrappedRemotingMessageBody : WrappedMessage, IServiceRemotingRequestMessageBody, IServiceRemotingResponseMessageBody
{

  public void SetParameter(
        int position,
        string parameName,
        object parameter)
    {
        throw new NotImplementedException();
    }

    public object GetParameter(
        int position,
        string parameName,
        Type paramType)
    {
        throw new NotImplementedException();
    }

    public void Set(
        object response)
    {
        throw new NotImplementedException();
    }

    public object Get(
        Type paramType)
    {
        throw new NotImplementedException();
    }

}

/// <summary>
/// TODO add documentation
/// </summary>
[DataContract(Name = "msgBodywrapped", Namespace = Constants.ServiceCommunicationNamespace)]
public abstract class WrappedMessage
{
    /// <summary>
    /// TODO add documentation 
    /// </summary>
    [DataMember(Name = "value", IsRequired = true, Order = 1)]
    public object Value
    {
        get;
        set;
    }
}
