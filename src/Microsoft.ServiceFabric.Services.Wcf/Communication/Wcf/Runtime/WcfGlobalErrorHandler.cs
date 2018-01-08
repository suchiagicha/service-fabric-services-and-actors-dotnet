﻿// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Communication.Wcf.Runtime
{
    using System;
    using System.ServiceModel;
    using System.ServiceModel.Channels;
    using System.ServiceModel.Dispatcher;

    internal class WcfGlobalErrorHandler : IErrorHandler
    {
        private readonly ChannelDispatcher channelDisp;

        public WcfGlobalErrorHandler(ChannelDispatcher channelDispatcher)
        {
            this.channelDisp = channelDispatcher;
        }

        public bool HandleError(Exception error)
        {
            if (error is FaultException)
            {
                return false;
            }

            return true;
        }

        public void ProvideFault(Exception error, MessageVersion version, ref Message fault)
        {
            if (error is FaultException)
            {
                return;
            }

            if (this.channelDisp.Listener.State != CommunicationState.Opened)
            {
                FaultCode faultCodeToUse = WcfRemoteExceptionInformation.FaultCodeRetry;
                var faultReason = new FaultReason(WcfRemoteExceptionInformation.ToString(error));
                var faultException = new FaultException(faultReason, faultCodeToUse);
                MessageFault mssgFault = faultException.CreateMessageFault();
                fault = Message.CreateMessage(version, mssgFault, null);
            }
        }
    }
}