// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    using System;
    using System.Fabric.Common;

    /// <summary>
    ///     Represents a change to an actor state with a given state name.
    /// </summary>
    public sealed class ActorStateChange
    {
        /// <summary>
        ///     Creates an instance of ActorStateChange class.
        /// </summary>
        /// <param name="stateName">The name of the actor state.</param>
        /// <param name="type">The type of value associated with given actor state name.</param>
        /// <param name="value">The value associated with given actor state name.</param>
        /// <param name="changeKind">The kind of state change for given actor state name.</param>
        public ActorStateChange(string stateName, Type type, object value, StateChangeKind changeKind)
        {
            Requires.Argument("stateName", stateName).NotNull();

            this.StateName = stateName;
            this.Type = type;
            this.Value = value;
            this.ChangeKind = changeKind;
        }

        /// <summary>
        ///     Gets the name of the actor state.
        /// </summary>
        /// <value>
        ///     The name of the actor state.
        /// </value>
        public string StateName { get; }

        /// <summary>
        ///     Gets the type of value associated with given actor state name.
        /// </summary>
        /// <value>
        ///     The type of value associated with given actor state name.
        /// </value>
        public Type Type { get; }

        /// <summary>
        ///     Gets the value associated with given actor state name.
        /// </summary>
        /// <value>
        ///     The value associated with given actor state name.
        /// </value>
        public object Value { get; }

        /// <summary>
        ///     Gets the kind of state change for given actor state name.
        /// </summary>
        /// <value>
        ///     The kind of state change for given actor state name.
        /// </value>
        public StateChangeKind ChangeKind { get; }
    }
}