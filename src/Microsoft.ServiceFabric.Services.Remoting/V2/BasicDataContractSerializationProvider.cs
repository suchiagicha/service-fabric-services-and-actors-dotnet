// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.V2
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;
    using Microsoft.ServiceFabric.Services.Remoting.V2.Messaging;

    class BasicDataContractSerializationProvider : IServiceRemotingMessageSerializationProvider
    {
        public IServiceRemotingMessageBodyFactory CreateMessageBodyFactory()
        {
            return new DataContractRemotingMessageFactory();
        }

        public IServiceRemotingRequestMessageBodySerializer CreateRequestMessageSerializer(Type serviceInterfaceType,
            IEnumerable<Type> requestBodyTypes)
        {
           return  new BasicDataRequestMessageBodySerializer(requestBodyTypes);
        }

        public IServiceRemotingResponseMessageBodySerializer CreateResponseMessageSerializer(Type serviceInterfaceType,
            IEnumerable<Type> responseBodyTypes)
        {
            return new BasicDataResponsetMessageBodySerializer(responseBodyTypes);
        }
    }

    class BasicDataRequestMessageBodySerializer : IServiceRemotingRequestMessageBodySerializer
    {
        private readonly DataContractSerializer serializer;

        public BasicDataRequestMessageBodySerializer(
            IEnumerable<Type> parameterInfo)
        {
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
            if (serviceRemotingRequestMessageBody == null )
            {
                return null;
            }

            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageBodyXmlDictionaryWriter(XmlDictionaryWriter.CreateBinaryWriter(stream)))
                {
                    serializer.WriteObject(writer, serviceRemotingRequestMessageBody);
                    writer.Flush();
                    var bytes = stream.ToArray();
                    var segments = new List<ArraySegment<byte>>();
                    segments.Add(new ArraySegment<byte>(bytes));
                    return  new OutgoingMessageBody(segments);
                }
            }
        }

        public IServiceRemotingRequestMessageBody Deserialize(IncomingMessageBody messageBody)
        {
            if ((messageBody == null) || (messageBody.GetReceivedBuffer() == null || messageBody.GetReceivedBuffer().Length==0))
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

    class BasicDataResponsetMessageBodySerializer : IServiceRemotingResponseMessageBodySerializer
    {
        private readonly DataContractSerializer serializer;

        public BasicDataResponsetMessageBodySerializer(
            IEnumerable<Type> parameterInfo)
        {
            this.serializer = new DataContractSerializer(
                typeof(ServiceRemotingResponseMessageBody),
                new DataContractSerializerSettings()
                {
                    MaxItemsInObjectGraph = int.MaxValue,
                    KnownTypes = parameterInfo
                });
        }
        public OutgoingMessageBody Serialize(IServiceRemotingResponseMessageBody serviceRemotingRequestMessageBody)
        {
            if (serviceRemotingRequestMessageBody == null)
            {
                return null;
            }

            using (var stream = new MemoryStream())
            {
                using (var writer = new MessageBodyXmlDictionaryWriter(XmlDictionaryWriter.CreateBinaryWriter(stream)))
                {
                    serializer.WriteObject(writer, serviceRemotingRequestMessageBody);
                    writer.Flush();
                    var bytes = stream.ToArray();
                    var segments = new List<ArraySegment<byte>>();
                    segments.Add(new ArraySegment<byte>(bytes));
                    return new OutgoingMessageBody(segments);
                }
            }
        }

        public IServiceRemotingResponseMessageBody Deserialize(IncomingMessageBody messageBody)
        {
            if ((messageBody == null) || (messageBody.GetReceivedBuffer() == null || messageBody.GetReceivedBuffer().Length == 0))
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
