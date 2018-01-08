// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Communication.Client
{
    using System;

    /// <summary>
    ///     Specifies the policy for retrying requests on exceptions in the communication channel between
    ///     client and service replicas.
    /// </summary>
    public sealed class OperationRetrySettings
    {
        /// <summary>
        ///     Instantiates OperationRetrySettings with default values for the retry settings.
        ///     The default values for MaxRetryBackoffIntervalOnTransientErrors, NonTransientErrors
        ///     are 2 seconds. The default value for MaxRetryCount is 10.
        /// </summary>
        public OperationRetrySettings()
            : this(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2), 10)
        {
        }

        /// <summary>
        ///     Instantiates OperationRetrySettings with the supplied settings.
        /// </summary>
        /// <param name="maxRetryBackoffIntervalOnTransientErrors">
        ///     Specifies the maximum interval to back-off before retrying incase of Transient errors
        /// </param>
        /// <param name="maxRetryBackoffIntervalOnNonTransientErrors">
        ///     Specifies the maximum interval to back-off before retrying incase of Non transient errors
        /// </param>
        /// <param name="defaultMaxRetryCount">
        ///     Specifies the maximum number of times to retry.
        /// </param>
        public OperationRetrySettings(
            TimeSpan maxRetryBackoffIntervalOnTransientErrors,
            TimeSpan maxRetryBackoffIntervalOnNonTransientErrors,
            int defaultMaxRetryCount)
        {
            this.MaxRetryBackoffIntervalOnTransientErrors = maxRetryBackoffIntervalOnTransientErrors;
            this.MaxRetryBackoffIntervalOnNonTransientErrors = maxRetryBackoffIntervalOnNonTransientErrors;
            this.DefaultMaxRetryCount = defaultMaxRetryCount;
        }

        /// <summary>
        ///     Specifies the maximum interval to back-off before retrying in-case of Transient errors
        /// </summary>
        /// <value>Maximum retry interval to back-off on transient errors</value>
        public TimeSpan MaxRetryBackoffIntervalOnTransientErrors { get; }

        /// <summary>
        ///     Specifies the maximum interval to back-off before retrying in-case of Non transient errors
        /// </summary>
        /// <value>Maximum retry interval to back-off on non transient errors</value>
        public TimeSpan MaxRetryBackoffIntervalOnNonTransientErrors { get; }

        /// <summary>
        ///     Specifies the maximum number of times to retry.
        /// </summary>
        /// <value>Maximum number of times to retry a specific exception.</value>
        public int DefaultMaxRetryCount { get; }
    }
}