// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.
// ------------------------------------------------------------------------------
// Changes to this file must follow the http://aka.ms/api-review process.
// ------------------------------------------------------------------------------

namespace System.Threading.Tasks.Dataflow
{
    public sealed partial class TransformManyBlock<TInput, TOutput>
    {
        public TransformManyBlock(System.Func<TInput, System.Collections.Generic.IAsyncEnumerable<TOutput>> transform) { }
        public TransformManyBlock(System.Func<TInput, System.Collections.Generic.IAsyncEnumerable<TOutput>> transform, System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions dataflowBlockOptions) { }
        public TransformManyBlock(System.Func<TInput, System.Threading.Tasks.Task<System.Collections.Generic.IAsyncEnumerable<TOutput>>> transform) { }
        public TransformManyBlock(System.Func<TInput, System.Threading.Tasks.Task<System.Collections.Generic.IAsyncEnumerable<TOutput>>> transform, System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions dataflowBlockOptions) { }
    }
}
