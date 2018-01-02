// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.

namespace Microsoft.ServiceFabric.Services.Remoting.V2
{
    using System;
    using System.Collections.Generic;
    using System.Fabric.Common;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Xml;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Client;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;

    /// <summary>
    /// This is the default implmentation  for <see cref="IServiceRemotingMessageSerializationProvider"/>used by remoting service and client during
    /// request/response serialization . It used DataContract for serialization.
    /// </summary>
    public class ServiceRemotingDataContractSerializationProvider : IServiceRemotingMessageSerializationProvider
    {
        
        private readonly IBufferPoolManager bodyBufferPoolManager;

        /// <summary>
        /// Creates a ServiceRemotingDataContractSerializationProvider with default IBufferPoolManager 
        /// </summary>
        public ServiceRemotingDataContractSerializationProvider()
            : this(new BufferPoolManager(Constants.DefaultMessageBufferSize,Constants.DefaultMaxBufferCount))
        {
        }

        /// <summary>
        /// Creates a ServiceRemotingDataContractSerializationProvider with user specified IBufferPoolManager
        /// </summary>
        /// <param name="bodyBufferPoolManager"></param>
        public ServiceRemotingDataContractSerializationProvider(
            IBufferPoolManager bodyBufferPoolManager)
        {
            this.bodyBufferPoolManager = bodyBufferPoolManager;
        }

        /// <summary>
        /// Creates IServiceRemotingRequestMessageBodySerializer for a serviceInterface using DataContract implementation
        /// </summary>
        /// <param name="serviceInterfaceType">User service interface</param>
        /// <param name="requestBodyTypes">Parameters for all the methods in the serviceInterfaceType</param>
        /// <returns></returns>
        public IServiceRemotingRequestMessageBodySerializer CreateRequestMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> requestBodyTypes)
        {
            return new ServiceRemotingRequestMessageBodySerializer(
                this.bodyBufferPoolManager,
                requestBodyTypes);
        }


        /// <summary>
        /// Creates IServiceRemotingResponseMessageBodySerializer for a serviceInterface using DataContract implementation
        /// </summary>
        /// <param name="serviceInterfaceType">User service interface</param>
        /// <param name="responseBodyTypes">Return Types for all the methods in the serviceInterfaceType</param>
        /// <returns></returns>
        public IServiceRemotingResponseMessageBodySerializer CreateResponseMessageSerializer(
            Type serviceInterfaceType,
            IEnumerable<Type> responseBodyTypes)
        {
            return new ServiceRemotingResponseMessageBodySerializer(
                this.bodyBufferPoolManager,
                responseBodyTypes);
        }
  
        /// <summary>
        /// Creates a MessageFactory for DataContract Remoting Types. This is used to create Remoting Request/Response objects.
        /// </summary>
        /// <returns></returns>
        public IServiceRemotingMessageBodyFactory CreateMessageBodyFactory()
        {
            return new DataContractRemotingMessageFactory();
        }

        internal class ServiceRemotingRequestMessageBodySerializer : IServiceRemotingRequestMessageBodySerializer
        {
            private readonly IBufferPoolManager bufferPoolManager;
            private readonly DataContractSerializer serializer;

            public ServiceRemotingRequestMessageBodySerializer(
                IBufferPoolManager bufferPoolManager,
                IEnumerable<Type>  parameterInfo)
            {
                this.bufferPoolManager = bufferPoolManager;
                this.serializer = new DataContractSerializer(
                    typeof(ServiceRemotingRequestMessageBody),
                    new DataContractSerializerSettings()
                    {
                        MaxItemsInObjectGraph = int.MaxValue,
                        KnownTypes = parameterInfo
                    });
            }

            public OutgoingMessageBody Serialize(IServiceRemotingRequestMessageBody serviceRemotingRequestMessageBody)
            {
                if (serviceRemotingRequestMessageBody == null)
                {
                    return null;
                }
               

                using (var stream = new SegmentedPoolMemoryStream(this.bufferPoolManager))
                {
                    using (var writer = new MessageBodyXmlDictionaryWriter(XmlDictionaryWriter.CreateBinaryWriter(stream)))
                    {
                        this.serializer.WriteObject(writer, serviceRemotingRequestMessageBody);
                        writer.Flush();
                        return new OutgoingMessageBody(stream.GetBuffers());
                    }
                }
            }

            public IServiceRemotingRequestMessageBody Deserialize(IncomingMessageBody messageBody)
            {
                if (messageBody == null || messageBody.GetReceivedBuffer()==null || messageBody.GetReceivedBuffer().Length==0)
                {
                    return null;
                }

                using (var reader = XmlDictionaryReader.CreateBinaryReader(
                    messageBody.GetReceivedBuffer(),
                    XmlDictionaryReaderQuotas.Max))
                {
                    return (ServiceRemotingRequestMessageBody)this.serializer.ReadObject(reader);
                
                }
            }
        }

        internal class ServiceRemotingResponseMessageBodySerializer : IServiceRemotingResponseMessageBodySerializer
        {
            private readonly IBufferPoolManager bufferPoolManager;
            private readonly DataContractSerializer serializer;

            public ServiceRemotingResponseMessageBodySerializer(
                IBufferPoolManager bufferPoolManager,
                IEnumerable<Type> parameterInfo)
            {
                this.bufferPoolManager = bufferPoolManager;
                this.serializer = new DataContractSerializer(
                    typeof(ServiceRemotingResponseMessageBody),
                    new DataContractSerializerSettings()
                    {
                        MaxItemsInObjectGraph = int.MaxValue,
                        KnownTypes = parameterInfo
                    });
            }

            public OutgoingMessageBody Serialize(IServiceRemotingResponseMessageBody serviceRemotingResponseMessageBody)
            {
                if (serviceRemotingResponseMessageBody == null)
                {
                    return null;
                }


                using (var stream = new SegmentedPoolMemoryStream(this.bufferPoolManager))
                {
                    using (var writer = new MessageBodyXmlDictionaryWriter(XmlDictionaryWriter.CreateBinaryWriter(stream)))
                    {
                        this.serializer.WriteObject(writer, serviceRemotingResponseMessageBody);
                        writer.Flush();
                        return new OutgoingMessageBody(stream.GetBuffers());
                    }
                }
            }

            public IServiceRemotingResponseMessageBody Deserialize(IncomingMessageBody messageBody)
            {
                if (messageBody == null || messageBody.GetReceivedBuffer() == null || messageBody.GetReceivedBuffer().Length == 0)
                {
                    return null;
                }

                using (var reader = XmlDictionaryReader.CreateBinaryReader(
                    messageBody.GetReceivedBuffer(),
                    XmlDictionaryReaderQuotas.Max))
                {
                    return (ServiceRemotingResponseMessageBody)this.serializer.ReadObject(reader);

                }
            }
        }

        class MessageBodyXmlDictionaryWriter : XmlDictionaryWriter
        {
            private readonly XmlDictionaryWriter wrapped;

            public MessageBodyXmlDictionaryWriter(XmlDictionaryWriter wrapped)
            {
                this.wrapped = wrapped;
            }

            public override WriteState WriteState
            {
                get
                {
                    return this.wrapped.WriteState;
                }
            }

            public override void Flush()
            {
                this.wrapped.Flush();
            }

            public override string LookupPrefix(string ns)
            {
                return wrapped.LookupPrefix(ns);
            }

            public override void WriteBase64(byte[] buffer, int index, int count)
            {
                this.wrapped.WriteBase64(buffer, index, count);
            }

            public override void WriteCData(string text)
            {
                this.wrapped.WriteCData(text);
            }

            public override void WriteCharEntity(char ch)
            {
                this.wrapped.WriteCharEntity(ch);
            }

            public override void WriteChars(char[] buffer, int index, int count)
            {
                this.wrapped.WriteChars(buffer, index, count);
            }

            public override void WriteComment(string text)
            {
                this.wrapped.WriteComment(text);
            }

            public override void WriteDocType(string name, string pubid, string sysid, string subset)
            {
                this.wrapped.WriteDocType(name, pubid, sysid, subset);
            }

            public override void WriteEndAttribute()
            {
                this.wrapped.WriteEndAttribute();
            }

            public override void WriteEndDocument()
            {
                this.wrapped.WriteEndDocument();
            }

            public override void WriteEndElement()
            {
                this.wrapped.WriteEndElement();
            }

            public override void WriteEntityRef(string name)
            {
                this.wrapped.WriteEntityRef(name);
            }

            public override void WriteFullEndElement()
            {
                this.wrapped.WriteFullEndElement();
            }

            public override void WriteProcessingInstruction(string name, string text)
            {
                this.wrapped.WriteProcessingInstruction(name, text);
            }

            public override void WriteRaw(string data)
            {
                this.wrapped.WriteRaw(data);
            }

            public override void WriteRaw(char[] buffer, int index, int count)
            {
                this.wrapped.WriteRaw(buffer, index, count);
            }

            public override void WriteStartAttribute(string prefix, string localName, string ns)
            {
                this.wrapped.WriteStartAttribute(prefix, localName, ns);
            }

            public override void WriteStartDocument()
            {
                this.wrapped.WriteStartDocument();
            }

            public override void WriteStartDocument(bool standalone)
            {
                this.wrapped.WriteStartDocument(standalone);
            }

            public override void WriteStartElement(string prefix, string localName, string ns)
            {
                if (string.IsNullOrEmpty(prefix) && (ns == Constants.ServiceCommunicationNamespace))
                {
                    prefix = "_x";
                }

                this.wrapped.WriteStartElement(prefix, localName, ns);
            }

            public override void WriteString(string text)
            {
                this.wrapped.WriteString(text);
            }

            public override void WriteSurrogateCharEntity(char lowChar, char highChar)
            {
                this.wrapped.WriteSurrogateCharEntity(lowChar, highChar);
            }

            public override void WriteWhitespace(string ws)
            {
                this.wrapped.WriteWhitespace(ws);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);

                if (disposing)
                {
                    wrapped.Dispose();
                }
            }
        }
    }

    [DataContract(Name = "msgResponse", Namespace = Constants.ServiceCommunicationNamespace)]
    internal class ServiceRemotingResponseMessageBody : IServiceRemotingResponseMessageBody
    {
        [DataMember] private object response;
        public void Set(object response)
        {
            this.response = response;
        }

        public object Get(Type paramType)
        {
            return this.response;
        }
    }
}