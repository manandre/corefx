// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//
// JoinManyBlock.cs
//
//
// Block that join multiple messages of same type from multiple sources together
// into an array.
//
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks.Dataflow.Internal;

namespace System.Threading.Tasks.Dataflow
{
    /// <summary>
    /// Provides a dataflow block that joins across multiple dataflow sources of the same type,
    /// waiting for one item to arrive for each source before they are all released together as an array.
    /// </summary>
    /// <typeparam name="T">Specifies the type of data accepted by the block targets</typeparam>
    [DebuggerDisplay("{DebuggerDisplayContent,nq}")]
    [DebuggerTypeProxy(typeof(JoinManyBlock<>.DebugView))]
    public sealed class JoinManyBlock<T> : IReceivableSourceBlock<T[]>, IDebuggerDisplay
    {
        /// <summary>Resources shared by all targets for this join block.</summary>
        private readonly JoinBlockTargetSharedResources _sharedResources;
        /// <summary>The source half of this join.</summary>
        private readonly SourceCore<T[]> _source;

        private readonly JoinBlockTarget<T>[] _targets;

        /// <summary>Initializes the <see cref="JoinManyBlock{T}"/>.</summary>
        /// <param name="joinNumber">The number of targets to join</param>
        public JoinManyBlock(int joinNumber) :
            this(joinNumber, new GroupingDataflowBlockOptions())
        { }

        /// <summary>Initializes the <see cref="JoinManyBlock{T}"/>.</summary>
        /// <param name="joinNumber">The number of targets to join</param>
        /// <param name="dataflowBlockOptions">The options with which to configure this <see cref="JoinManyBlock{T}"/>.</param>
        /// <exception cref="System.ArgumentNullException">The <paramref name="dataflowBlockOptions"/> is null (Nothing in Visual Basic).</exception>
        public JoinManyBlock(int joinNumber, GroupingDataflowBlockOptions dataflowBlockOptions)
        {
            // Validate arguments
            if (joinNumber < 1) throw new ArgumentOutOfRangeException(nameof(joinNumber), joinNumber, SR.ArgumentOutOfRange_GenericPositive);
            if (dataflowBlockOptions == null) throw new ArgumentNullException(nameof(dataflowBlockOptions));
            Contract.EndContractBlock();

            // Ensure we have options that can't be changed by the caller
            dataflowBlockOptions = dataflowBlockOptions.DefaultOrClone();

            // Initialize bounding state if necessary
            Action<ISourceBlock<T[]>, int> onItemsRemoved = null;
            if (dataflowBlockOptions.BoundedCapacity > 0) onItemsRemoved = (owningSource, count) => ((JoinManyBlock<T>)owningSource)._sharedResources.OnItemsRemoved(count);

            // Configure the source
            _source = new SourceCore<T[]>(this, dataflowBlockOptions,
                owningSource => ((JoinManyBlock<T>)owningSource)._sharedResources.CompleteEachTarget(),
                onItemsRemoved);

            // Configure targets
            _targets = new JoinBlockTarget<T>[joinNumber];
            _sharedResources = new JoinBlockTargetSharedResources(this, _targets,
                () =>
                {
                    _source.AddMessage(_targets.Select(t => t.GetOneMessage()).ToArray());
                },
                exception =>
                {
                    Volatile.Write(ref _sharedResources._hasExceptions, true);
                    _source.AddException(exception);
                },
                dataflowBlockOptions);

            for (int i = 0; i < joinNumber; i++)
            {
                _targets[i] = new JoinBlockTarget<T>(_sharedResources);
            }

            // Let the source know when all targets have completed
            Task.Factory.ContinueWhenAll(
                _targets.Select(t => t.CompletionTaskInternal).ToArray(),
                _ => _source.Complete(),
                CancellationToken.None, Common.GetContinuationOptions(), TaskScheduler.Default);

            // It is possible that the source half may fault on its own, e.g. due to a task scheduler exception.
            // In those cases we need to fault the target half to drop its buffered messages and to release its
            // reservations. This should not create an infinite loop, because all our implementations are designed
            // to handle multiple completion requests and to carry over only one.
            _source.Completion.ContinueWith((completed, state) =>
            {
                var thisBlock = ((JoinManyBlock<T>)state) as IDataflowBlock;
                Debug.Assert(completed.IsFaulted, "The source must be faulted in order to trigger a target completion.");
                thisBlock.Fault(completed.Exception);
            }, this, CancellationToken.None, Common.GetContinuationOptions() | TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);

            // Handle async cancellation requests by declining on the target
            Common.WireCancellationToComplete(
                dataflowBlockOptions.CancellationToken, _source.Completion, state => ((JoinManyBlock<T>)state)._sharedResources.CompleteEachTarget(), this);
#if FEATURE_TRACING
            DataflowEtwProvider etwLog = DataflowEtwProvider.Log;
            if (etwLog.IsEnabled())
            {
                etwLog.DataflowBlockCreated(this, dataflowBlockOptions);
            }
#endif
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="LinkTo"]/*' />
        public IDisposable LinkTo(ITargetBlock<T[]> target, DataflowLinkOptions linkOptions)
        {
            return _source.LinkTo(target, linkOptions);
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="TryReceive"]/*' />
        public bool TryReceive(Predicate<T[]> filter, out T[] item)
        {
            return _source.TryReceive(filter, out item);
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="TryReceiveAll"]/*' />
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public bool TryReceiveAll(out IList<T[]> items) { return _source.TryReceiveAll(out items); }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="OutputCount"]/*' />
        public int OutputCount { get { return _source.OutputCount; } }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Blocks/Member[@name="Completion"]/*' />
        public Task Completion { get { return _source.Completion; } }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Blocks/Member[@name="Complete"]/*' />
        public void Complete()
        {
            Array.ForEach(_targets, t => t.CompleteCore(exception: null, dropPendingMessages: false, releaseReservedMessages: false));
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Blocks/Member[@name="Fault"]/*' />
        void IDataflowBlock.Fault(Exception exception)
        {
            if (exception == null) throw new ArgumentNullException(nameof(exception));
            Contract.EndContractBlock();

            Debug.Assert(_sharedResources != null, "_sharedResources not initialized");
            Debug.Assert(_sharedResources._exceptionAction != null, "_sharedResources._exceptionAction not initialized");

            lock (_sharedResources.IncomingLock)
            {
                if (!_sharedResources._decliningPermanently) _sharedResources._exceptionAction(exception);
            }

            Complete();
        }

        public ITargetBlock<T>[] Targets => _targets;

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="ConsumeMessage"]/*' />
        T[] ISourceBlock<T[]>.ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target, out bool messageConsumed)
        {
            return _source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="ReserveMessage"]/*' />
        bool ISourceBlock<T[]>.ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            return _source.ReserveMessage(messageHeader, target);
        }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Sources/Member[@name="ReleaseReservation"]/*' />
        void ISourceBlock<T[]>.ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            _source.ReleaseReservation(messageHeader, target);
        }

        /// <summary>Gets the number of messages waiting to be processed.  This must only be used from the debugger as it avoids taking necessary locks.</summary>
        private int OutputCountForDebugger { get { return _source.GetDebuggingInformation().OutputCount; } }

        /// <include file='XmlDocs/CommonXmlDocComments.xml' path='CommonXmlDocComments/Blocks/Member[@name="ToString"]/*' />
        public override string ToString() { return Common.GetNameForDebugger(this, _source.DataflowBlockOptions); }

        /// <summary>The data to display in the debugger display attribute.</summary>
        [SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider")]
        private object DebuggerDisplayContent
        {
            get
            {
                return string.Format("{0}, OutputCount={1}",
                    Common.GetNameForDebugger(this, _source.DataflowBlockOptions),
                    OutputCountForDebugger);
            }
        }
        /// <summary>Gets the data to display in the debugger display attribute for this instance.</summary>
        object IDebuggerDisplay.Content { get { return DebuggerDisplayContent; } }

        /// <summary>Provides a debugger type proxy for the JoinBlock.</summary>
        private sealed class DebugView
        {
            /// <summary>The JoinManyBlock being viewed.</summary>
            private readonly JoinManyBlock<T> _joinManyBlock;
            /// <summary>The source half of the block being viewed.</summary>
            private readonly SourceCore<T[]>.DebuggingInformation _sourceDebuggingInformation;

            /// <summary>Initializes the debug view.</summary>
            /// <param name="joinManyBlock">The JoinManyBlock being viewed.</param>
            public DebugView(JoinManyBlock<T> joinManyBlock)
            {
                Debug.Assert(joinManyBlock != null, "Need a block with which to construct the debug view.");
                _joinManyBlock = joinManyBlock;
                _sourceDebuggingInformation = joinManyBlock._source.GetDebuggingInformation();
            }

            /// <summary>Gets the messages waiting to be received.</summary>
            public IEnumerable<T[]> OutputQueue { get { return _sourceDebuggingInformation.OutputQueue; } }
            /// <summary>Gets the number of joins created thus far.</summary>
            public long JoinsCreated { get { return _joinManyBlock._sharedResources._joinsCreated; } }

            /// <summary>Gets the task being used for input processing.</summary>
            public Task TaskForInputProcessing { get { return _joinManyBlock._sharedResources._taskForInputProcessing; } }
            /// <summary>Gets the task being used for output processing.</summary>
            public Task TaskForOutputProcessing { get { return _sourceDebuggingInformation.TaskForOutputProcessing; } }

            /// <summary>Gets the GroupingDataflowBlockOptions used to configure this block.</summary>
            public GroupingDataflowBlockOptions DataflowBlockOptions { get { return (GroupingDataflowBlockOptions)_sourceDebuggingInformation.DataflowBlockOptions; } }
            /// <summary>Gets whether the block is declining further messages.</summary>
            public bool IsDecliningPermanently { get { return _joinManyBlock._sharedResources._decliningPermanently; } }
            /// <summary>Gets whether the block is completed.</summary>
            public bool IsCompleted { get { return _sourceDebuggingInformation.IsCompleted; } }
            /// <summary>Gets the block's Id.</summary>
            public int Id { get { return Common.GetBlockId(_joinManyBlock); } }
            /// <summary>Gets the targets.</summary>
            public ITargetBlock<T>[] Targets => _joinManyBlock._targets;
            /// <summary>Gets the set of all targets linked from this block.</summary>
            public TargetRegistry<T[]> LinkedTargets { get { return _sourceDebuggingInformation.LinkedTargets; } }
            /// <summary>Gets the set of all targets linked from this block.</summary>
            public ITargetBlock<T[]> NextMessageReservedFor { get { return _sourceDebuggingInformation.NextMessageReservedFor; } }
        }
    }
}
