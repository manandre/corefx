// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//
// TransformManyBlock.cs
//
//
// A propagator block that runs a function on each input to produce zero or more outputs.
//
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace System.Threading.Tasks.Dataflow
{

    public sealed partial class TransformManyBlock<TInput, TOutput>
    {
        /// <summary>Initializes the <see cref="TransformManyBlock{TInput,TOutput}"/> with the specified function.</summary>
        /// <param name="transform">
        /// The function to invoke with each data element received.  All of the data from the returned <see cref="System.Collections.Generic.IAsyncEnumerable{TOutput}"/>
        /// will be made available as output from this <see cref="TransformManyBlock{TInput,TOutput}"/>.
        /// </param>
        /// <exception cref="System.ArgumentNullException">The <paramref name="transform"/> is null (Nothing in Visual Basic).</exception>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public TransformManyBlock(Func<TInput, IAsyncEnumerable<TOutput>> transform) :
            this(transform, ExecutionDataflowBlockOptions.Default)
        { }

        /// <summary>Initializes the <see cref="TransformManyBlock{TInput,TOutput}"/> with the specified function and <see cref="ExecutionDataflowBlockOptions"/>.</summary>
        /// <param name="transform">
        /// The function to invoke with each data element received.  All of the data from the returned in the <see cref="System.Collections.Generic.IAsyncEnumerable{TOutput}"/>
        /// will be made available as output from this <see cref="TransformManyBlock{TInput,TOutput}"/>.
        /// </param>
        /// <param name="dataflowBlockOptions">The options with which to configure this <see cref="TransformManyBlock{TInput,TOutput}"/>.</param>
        /// <exception cref="System.ArgumentNullException">The <paramref name="transform"/> is null (Nothing in Visual Basic).</exception>
        /// <exception cref="System.ArgumentNullException">The <paramref name="dataflowBlockOptions"/> is null (Nothing in Visual Basic).</exception>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public TransformManyBlock(Func<TInput, IAsyncEnumerable<TOutput>> transform, ExecutionDataflowBlockOptions dataflowBlockOptions) :
            this(t => transform(t)?.ToEnumerable(), dataflowBlockOptions)
        {
            if (transform == null) throw new ArgumentNullException(nameof(transform));
        }
    }
}
