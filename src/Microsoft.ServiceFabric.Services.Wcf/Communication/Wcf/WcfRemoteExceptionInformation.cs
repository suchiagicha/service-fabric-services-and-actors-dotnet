// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Communication.Wcf
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.ServiceModel;
    using System.Text;
    using System.Xml;
    using Microsoft.ServiceFabric.Services.Wcf;

    internal class WcfRemoteExceptionInformation
    {
        public static readonly string FaultCodeName = "WcfRemoteExceptionInformation";
        public static readonly string FaultSubCodeRetryName = "Retry";
        public static readonly string FaultSubCodeThrowName = "Throw";

        public static readonly FaultCode FaultCodeRetry = new FaultCode(
            FaultCodeName,
            new FaultCode(FaultSubCodeRetryName));

        public static readonly FaultCode FaultCodeThrow = new FaultCode(
            FaultCodeName,
            new FaultCode(FaultSubCodeThrowName));

        private static readonly DataContractSerializer ServiceExceptionDataSerializer =
            new DataContractSerializer(typeof(ServiceExceptionData));

        public static string ToString(Exception exception)
        {
            try
            {
                var exceptionSerializer = new NetDataContractSerializer();

                var stringWriter = new StringWriter();

                using (XmlWriter textStream = XmlWriter.Create(stringWriter))
                {
                    exceptionSerializer.WriteObject(textStream, exception);
                    textStream.Flush();

                    return stringWriter.ToString();
                }
            }
            catch (Exception)
            {
                var exceptionStringBuilder = new StringBuilder();

                exceptionStringBuilder.AppendFormat(
                    CultureInfo.CurrentCulture,
                    SR.ErrorExceptionSerializationFailed1,
                    exception.GetType().FullName);

                exceptionStringBuilder.AppendLine();

                exceptionStringBuilder.AppendFormat(
                    CultureInfo.CurrentCulture,
                    SR.ErrorExceptionSerializationFailed2,
                    exception);

                var exceptionData = new ServiceExceptionData(
                    exception.GetType().FullName,
                    exceptionStringBuilder.ToString());
                string result;
                if (TrySerializeExceptionData(exceptionData, out result))
                {
                    return result;
                }

                throw;
            }
        }


        public static Exception ToException(string exceptionString)
        {
            try
            {
                var exceptionSerializer = new NetDataContractSerializer();
                var stringReader = new StringReader(exceptionString);

                // disabling DTD processing on XML streams that are not over the network.
                var settings = new XmlReaderSettings
                {
                    DtdProcessing = DtdProcessing.Prohibit,
                    XmlResolver = null
                };
                using (XmlReader textStream = XmlReader.Create(stringReader, settings))
                {
                    return (Exception) exceptionSerializer.ReadObject(textStream);
                }
            }
            catch (Exception)
            {
                // add the message as service exception
                ServiceExceptionData exceptionData;
                if (TryDeserializeExceptionData(exceptionString, out exceptionData))
                {
                    return new ServiceException(exceptionData.Type, exceptionData.Message);
                }

                throw;
            }
        }

        private static bool TrySerializeExceptionData(ServiceExceptionData serviceExceptionData, out string result)
        {
            try
            {
                var stringWriter = new StringWriter();

                using (XmlWriter textStream = XmlWriter.Create(stringWriter))
                {
                    ServiceExceptionDataSerializer.WriteObject(textStream, serviceExceptionData);
                    textStream.Flush();

                    result = stringWriter.ToString();
                    return true;
                }
            }
            catch (Exception)
            {
                // no-op
            }

            result = null;
            return false;
        }

        private static bool TryDeserializeExceptionData(string exceptionString, out ServiceExceptionData result)
        {
            try
            {
                var stringReader = new StringReader(exceptionString);

                // disabling DTD processing on XML streams that are not over the network.
                var settings = new XmlReaderSettings
                {
                    DtdProcessing = DtdProcessing.Prohibit,
                    XmlResolver = null
                };
                using (XmlReader textStream = XmlReader.Create(stringReader, settings))
                {
                    result = (ServiceExceptionData) ServiceExceptionDataSerializer.ReadObject(textStream);
                    return true;
                }
            }
            catch (Exception)
            {
                // no-op
            }

            result = null;
            return false;
        }
    }
}