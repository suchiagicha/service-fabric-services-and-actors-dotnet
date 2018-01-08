// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Client
{
    using System.Fabric;

    /// <summary>
    ///     Defines a key to address a service partition.
    /// </summary>
    public sealed class ServicePartitionKey
    {
        /// <summary>
        ///     Returns a ServicePartitionKey that indicates a singleton partition.
        /// </summary>
        public static readonly ServicePartitionKey Singleton = new ServicePartitionKey();

        /// <summary>
        ///     Instantiates a ServicePartitionKey for singleton partitioned service.
        /// </summary>
        public ServicePartitionKey()
        {
            this.Value = null;
            this.Kind = ServicePartitionKind.Singleton;
        }

        /// <summary>
        ///     Instantiates a ServicePartitionKey for uniform int64 partitioned service.
        /// </summary>
        /// <param name="partitionKey">Value of the int64 partition key</param>
        public ServicePartitionKey(long partitionKey)
        {
            this.Kind = ServicePartitionKind.Int64Range;
            this.Value = partitionKey;
        }

        /// <summary>
        ///     Instantiates a ServicePartitionKey for named partitioned services.
        /// </summary>
        /// <param name="partitionKey">Value of the named partition key</param>
        public ServicePartitionKey(string partitionKey)
        {
            this.Kind = ServicePartitionKind.Named;
            this.Value = partitionKey;
        }

        /// <summary>
        ///     Gets the Kind of the partition key applies to.
        /// </summary>
        /// <value>Partition kind</value>
        public ServicePartitionKind Kind { get; }

        /// <summary>
        ///     Gets the value of the partition key. This value can be casted to the right type based on the value of the Kind
        ///     property.
        /// </summary>
        /// <value>Partition key</value>
        public object Value { get; }
    }
}