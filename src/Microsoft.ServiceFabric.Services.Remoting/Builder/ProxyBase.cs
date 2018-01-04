// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Remoting.Builder
{
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Services.Common;
    using Microsoft.ServiceFabric.Services.Remoting.V2;

    /// <summary>
    ///     The base class used by remoting code generator to generate the proxy for the remoted interfaces.
    /// </summary>
    public abstract class ProxyBase
    {
        internal IServiceRemotingMessageBodyFactory serviceRemotingMessageBodyFactory;

        /// <summary>
        ///     Called by the generated proxy class to continue after getting the response body that does not have value.
        /// </summary>
        /// <param name="task">A task that represents the asynchronous operation for remote method call.</param>
        /// <returns>A task that represents the asynchronous operation for remote method call.</returns>
        protected Task ContinueWith(Task<object> task)
        {
            return task;
        }


        /// <summary>
        ///     Creates the Remoting request message Body
        /// </summary>
        /// <param name="interfaceName">Full Name of the service interface for which this call is invoked</param>
        /// <param name="methodName">Method Name of the service interface for which this call is invoked</param>
        /// <param name="parameterCount">Number of Parameters in the service interface Method</param>
        /// <returns></returns>
        protected virtual IServiceRemotingRequestMessageBody CreateRequestMessageBodyV2(
            string interfaceName, string methodName,
            int parameterCount)
        {
            return this.serviceRemotingMessageBodyFactory.CreateRequest(interfaceName, methodName, parameterCount);
        }

        /// <summary>
        ///     Called by the generated proxy class to get the result from the response body.
        /// </summary>
        /// <typeparam name="TRetval"><see cref="System.Type" /> of the remote method return value.</typeparam>
        /// <param name="task">A task that represents the asynchronous operation for remote method call.</param>
        /// <returns>
        ///     A task that represents the asynchronous operation for remote method call.
        ///     The value of the TRetval contains the remote method return value.
        /// </returns>
        protected async Task<TRetval> ContinueWithResultV2<TRetval>(
            Task<IServiceRemotingResponseMessageBody> task)
        {
            IServiceRemotingResponseMessageBody responseBody = await task;
            return (TRetval) responseBody.Get(typeof(TRetval));
        }

        /// <summary>
        ///     Called by the generated proxy class to send the request to the remote object and get the response back.
        /// </summary>
        /// <param name="interfaceId">Id of the remote interface.</param>
        /// <param name="methodId">Id of the remote method to be invokved.</param>
        /// <param name="requestMsgBodyValue">Request body.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that represents the asynchronous operation async call to remote object.</returns>
        protected async Task<IServiceRemotingResponseMessageBody> InvokeAsyncV2(
            int interfaceId,
            int methodId,
            IServiceRemotingRequestMessageBody requestMsgBodyValue,
            CancellationToken cancellationToken)
        {
            IServiceRemotingResponseMessage responseMsg = await this.InvokeAsyncImplV2(
                interfaceId,
                methodId,
                requestMsgBodyValue,
                cancellationToken);

            return responseMsg != null
                ? responseMsg.GetBody()
                : null;
        }


        /// <summary>
        ///     Called by the generated proxy class to send the requestMessage to the remote object.
        /// </summary>
        /// <param name="interfaceId">Id of the remote interface.</param>
        /// <param name="methodId">Id of the remote method to be invokved.</param>
        /// <param name="requestMsgBodyValue">Message body to be sent to remote object.</param>
        protected void InvokeV2(
            int interfaceId,
            int methodId,
            IServiceRemotingRequestMessageBody requestMsgBodyValue)
        {
            this.InvokeImplV2(
                interfaceId,
                methodId,
                requestMsgBodyValue);
        }

        //V2 Stack Api

        internal void InitializeV2(
            IServiceRemotingMessageBodyFactory serviceRemotingMessageBodyFactory)
        {
            this.serviceRemotingMessageBodyFactory = serviceRemotingMessageBodyFactory;
        }


        internal abstract Task<IServiceRemotingResponseMessage> InvokeAsyncImplV2(
            int interfaceId,
            int methodId,
            IServiceRemotingRequestMessageBody requestMsgBodyValue,
            CancellationToken cancellationToken);

        internal abstract void InvokeImplV2(
            int interfaceId,
            int methodId,
            IServiceRemotingRequestMessageBody requestMsgBodyValue);

#if !DotNetCoreClr
        /// <summary>
        ///     Called by the generated proxy class to send the message to the remote object.
        /// </summary>
        /// <param name="interfaceId">Id of the remote interface.</param>
        /// <param name="methodId">Id of the remote method to be invokved.</param>
        /// <param name="requestMsgBodyValue">Message body to be sent to remote object.</param>
        protected void Invoke(
            int interfaceId,
            int methodId,
            object requestMsgBodyValue)
        {
            object requestMsgBody = null;
            if (requestMsgBodyValue != null)
            {
                requestMsgBody = this.CreateRequestMessageBody(requestMsgBodyValue);
            }

            byte[] requestMsgBodyBytes = SerializationUtility.Serialize(
                this.GetRequestMessageBodySerializer(interfaceId),
                requestMsgBody);

            this.Invoke(interfaceId, methodId, requestMsgBodyBytes);
        }

        /// <summary>
        ///     Called by the generated proxy class to send the request to the remote object and get the response back.
        /// </summary>
        /// <param name="interfaceId">Id of the remote interface.</param>
        /// <param name="methodId">Id of the remote method to be invokved.</param>
        /// <param name="requestMsgBodyValue">Request body.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that represents the asynchronous operation async call to remote object.</returns>
        protected async Task<object> InvokeAsync(
            int interfaceId,
            int methodId,
            object requestMsgBodyValue,
            CancellationToken cancellationToken)
        {
            object requestMsgBody = null;
            if (requestMsgBodyValue != null)
            {
                requestMsgBody = this.CreateRequestMessageBody(requestMsgBodyValue);
            }

            byte[] requestMsgBodyBytes = SerializationUtility.Serialize(
                this.GetRequestMessageBodySerializer(interfaceId),
                requestMsgBody);

            byte[] responseMsgBodyBytes = await this.InvokeAsync(
                interfaceId,
                methodId,
                requestMsgBodyBytes,
                cancellationToken);

            object responseMsgBody = SerializationUtility.Deserialize(
                this.GetResponseMessageBodySerializer(interfaceId),
                responseMsgBodyBytes);

            return responseMsgBody != null ? this.GetResponseMessageBodyValue(responseMsgBody) : null;
        }

        /// <summary>
        ///     Called by the generated proxy class to get the result from the response body.
        /// </summary>
        /// <typeparam name="TRetval"><see cref="System.Type" /> of the remote method return value.</typeparam>
        /// <param name="interfaceId">Interface Id for the remoted interface.</param>
        /// <param name="methodId">Method Id for the remote method.</param>
        /// <param name="task">A task that represents the asynchronous operation for remote method call.</param>
        /// <returns>
        ///     A task that represents the asynchronous operation for remote method call.
        ///     The value of the TRetval contains the remote method return value.
        /// </returns>
        protected async Task<TRetval> ContinueWithResult<TRetval>(
            int interfaceId,
            int methodId,
            Task<object> task)
        {
            object responseBody = await task;
            return (TRetval) this.GetReturnValue(interfaceId, methodId, responseBody);
        }


        /// <summary>
        ///     Implemented by the derived class to type cast the response body and extract the value from it.
        /// </summary>
        /// <param name="interfaceId">Interface Id for the actor interface.</param>
        /// <param name="methodId">Method Id for the actor method.</param>
        /// <param name="responseBody">Response body.</param>
        /// <returns>Return value of method call as <see cref="System.Object" />.</returns>
        protected abstract object GetReturnValue(int interfaceId, int methodId, object responseBody);

        internal abstract DataContractSerializer GetRequestMessageBodySerializer(int interfaceId);

        internal abstract DataContractSerializer GetResponseMessageBodySerializer(int interfaceId);

        internal abstract object GetResponseMessageBodyValue(object responseMessageBody);

        internal abstract object CreateRequestMessageBody(object requestMessageBodyValue);

        internal abstract Task<byte[]> InvokeAsync(int interfaceId, int methodId, byte[] requestMsgBodyBytes, CancellationToken cancellationToken);

        internal abstract void Invoke(int interfaceId, int methodId, byte[] requestMsgBodyBytes);

#endif
    }
}