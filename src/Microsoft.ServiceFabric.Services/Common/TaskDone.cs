// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Common
{
    using System.Threading.Tasks;

    internal static class TaskDone
    {
        private static readonly Task<bool> DoneConstant = Task.FromResult(true);

        public static Task Done => DoneConstant;
    }

    internal static class TaskDone<T>
    {
        public static Task<T> Done { get; } = Task.FromResult(default(T));
    }
}