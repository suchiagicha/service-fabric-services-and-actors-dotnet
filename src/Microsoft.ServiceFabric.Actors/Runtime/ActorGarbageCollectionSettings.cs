// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    using System;

    /// <summary>
    ///     Represents the setting to configure Garbage Collection behavior of Actor Service.
    /// </summary>
    public sealed class ActorGarbageCollectionSettings
    {
        /// <summary>
        ///     Initializes a new instance of the ActorGarbageCollectionSettings class with the values of the input argument.
        /// </summary>
        public ActorGarbageCollectionSettings()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the ActorGarbageCollectionSettings class.
        /// </summary>
        /// <param name="idleTimeoutInSeconds">Time interval to wait before garbage collecting an actor which is not in use.</param>
        /// <param name="scanIntervalInSeconds">Time interval to run Actor Garbage Collection scan.</param>
        /// <exception cref="System.ArgumentOutOfRangeException">
        ///     <para>When idleTimeoutInSeconds is less than or equal to 0.</para>
        ///     <para>When scanIntervalInSeconds is less than or equal to 0.</para>
        ///     <para>When idleTimeoutInSeconds is less than scanIntervalInSeconds.</para>
        /// </exception>
        public ActorGarbageCollectionSettings(long idleTimeoutInSeconds, long scanIntervalInSeconds)
        {
            // Verify that values are within acceptable range.
            if (idleTimeoutInSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException("idleTimeoutInSeconds)", SR.ActorGCSettingsValueOutOfRange);
            }

            if (scanIntervalInSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException("scanIntervalInSeconds)", SR.ActorGCSettingsValueOutOfRange);
            }

            if (idleTimeoutInSeconds / scanIntervalInSeconds >= 1)
            {
                this.ScanIntervalInSeconds = scanIntervalInSeconds;
                this.IdleTimeoutInSeconds = idleTimeoutInSeconds;
            }
            else
            {
                throw new ArgumentOutOfRangeException(SR.ActorGCSettingsNotValid);
            }
        }

        /// <summary>
        ///     Initializes a new instance of the ActorGarbageCollectionSettings class.
        /// </summary>
        /// <param name="settings">The setting of Actor Garbage Collection.</param>
        internal ActorGarbageCollectionSettings(ActorGarbageCollectionSettings settings)
        {
            this.IdleTimeoutInSeconds = settings.IdleTimeoutInSeconds;
            this.ScanIntervalInSeconds = settings.ScanIntervalInSeconds;
        }

        /// <summary>
        ///     Gets the time interval to run Actor Garbage Collection scan.
        /// </summary>
        /// <value>The time interval in <see cref="System.Int64" /> to run Actor Garbage Collection scan.</value>
        public long ScanIntervalInSeconds { get; } = 60;

        /// <summary>
        ///     Gets the time interval to wait before garbage collecting an actor which is not in use.
        /// </summary>
        /// <value>The time interval in <see cref="System.Int64" /> to wait before garbage collecting an actor which is not in use.</value>
        public long IdleTimeoutInSeconds { get; } = 3600;
    }
}