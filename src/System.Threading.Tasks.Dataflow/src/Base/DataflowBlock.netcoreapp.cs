// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//
// DataflowBlock.netcoreapp.cs
//
//
// Common functionality for ITargetBlock, ISourceBlock, and IPropagatorBlock.
//
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace System.Threading.Tasks.Dataflow
{
    public static partial class DataflowBlock
    {
        /// <summary>Creates an <see cref="IAsyncEnumerable{T}"/> that enables receiving all of the data from the source.</summary>
        /// <typeparam name="TOutput">Specifies the type of data contained in the source.</typeparam>
        /// <param name="source">The source from which to asynchronously receive.</param>
        /// <param name="cancellationToken">The <see cref="System.Threading.CancellationToken"/> which may be used to cancel the receive operation.</param>
        /// <returns>The created async enumerable.</returns>
        /// <exception cref="System.ArgumentNullException">The <paramref name="source"/> is null (Nothing in Visual Basic).</exception>
        public static IAsyncEnumerable<TOutput> ReceiveAllAsync<TOutput>(this ISourceBlock<TOutput> source, CancellationToken cancellationToken = default)
        {
            // Validate arguments
            if (source == null) throw new ArgumentNullException(nameof(source));

            return source.ReceiveAllAsyncCore(cancellationToken);
        }

        private static async IAsyncEnumerable<TOutput> ReceiveAllAsyncCore<TOutput>(this ISourceBlock<TOutput> source, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (await source.OutputAvailableAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return await source.ReceiveAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
