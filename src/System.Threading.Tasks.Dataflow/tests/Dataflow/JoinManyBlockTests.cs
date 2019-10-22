// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace System.Threading.Tasks.Dataflow.Tests
{
    public class JoinManyBlockTests
    {
        [Fact]
        public void TestCtor()
        {
            var blocks = new[]
            {
                new JoinManyBlock<int>(2),
                new JoinManyBlock<int>(2,new GroupingDataflowBlockOptions { MaxNumberOfGroups = 1 }),
                new JoinManyBlock<int>(2,new GroupingDataflowBlockOptions { MaxMessagesPerTask = 1 }),
                new JoinManyBlock<int>(2,new GroupingDataflowBlockOptions { MaxMessagesPerTask = 1, CancellationToken = new CancellationToken(true), MaxNumberOfGroups = 1 })
            };
            foreach (var block in blocks)
            {
                Assert.Equal(expected: 2, actual: block.Targets.Length);
                Assert.Equal(expected: 0, actual: block.OutputCount);
                Assert.NotNull(block.Completion);
            }
        }

        [Fact]
        public void TestToString()
        {
            DataflowTestHelpers.TestToString(
                nameFormat => nameFormat != null ?
                    new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions() { NameFormat = nameFormat }) :
                    new JoinManyBlock<int>(2));
        }

        [Fact]
        public void TestArgumentExceptions()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new JoinManyBlock<int>(0));
            Assert.Throws<ArgumentNullException>(() => new JoinManyBlock<int>(2, null));
            Assert.Throws<NotSupportedException>(() => { var ignored = new JoinManyBlock<int>(2).Targets[0].Completion; });
            Assert.Throws<ArgumentNullException>(() => new JoinManyBlock<int>(2).Targets[0].Fault(null));
            AssertExtensions.Throws<ArgumentException>("messageHeader", () => new JoinManyBlock<int>(2).Targets[0].OfferMessage(default, 1, null, false));
            AssertExtensions.Throws<ArgumentException>("consumeToAccept", () => new JoinManyBlock<int>(2).Targets[0].OfferMessage(new DataflowMessageHeader(1), 1, null, true));

            DataflowTestHelpers.TestArgumentsExceptions<int[]>(new JoinManyBlock<int>(2));
        }

        [Fact]
        public async Task TestPostThenReceive()
        {
            const int Iters = 3;

            var block2 = new JoinManyBlock<int>(2);
            for (int i = 0; i < Iters; i++)
            {
                block2.Targets[0].Post(i);
                block2.Targets[1].Post(i + 1);

                int[] msg = await block2.ReceiveAsync();
                Assert.Equal(expected: i, actual: msg[0]);
                Assert.Equal(expected: i + 1, actual: msg[1]);
            }
        }

        [Fact]
        public async Task TestPostAllThenReceive()
        {
            int iter = 2;

            var block2 = new JoinManyBlock<int>(2);
            for (int i = 0; i < iter; i++)
            {
                block2.Targets[0].Post(i);
                block2.Targets[1].Post(i + 1);
            }
            for (int i = 0; i < iter; i++)
            {
                int[] msg = await block2.ReceiveAsync();
                Assert.Equal(expected: i, actual: msg[0]);
                Assert.Equal(expected: i + 1, actual: msg[1]);
            }
        }

        [Fact]
        public async Task TestSendAllThenReceive()
        {
            int iter = 2;
            Task<bool> t1, t2;

            var block2 = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });
            for (int i = 0; i < iter; i++)
            {
                t1 = block2.Targets[0].SendAsync(i);
                Assert.False(t1.IsCompleted);
                t2 = block2.Targets[1].SendAsync(i + 1);
                await Task.WhenAll(t1, t2);
            }
            for (int i = 0; i < iter; i++)
            {
                int[] msg = await block2.ReceiveAsync();
                Assert.Equal(expected: i, actual: msg[0]);
                Assert.Equal(expected: i + 1, actual: msg[1]);
            }
        }

        [Fact]
        public void TestOneTargetInsufficient()
        {
            var block2 = new JoinManyBlock<int>(2);
            block2.Targets[0].Post(0);

            Assert.False(block2.TryReceive(out int[] _));
            Assert.Equal(expected: 0, actual: block2.OutputCount);
        }

        [Fact]
        public async Task TestPrecancellation2()
        {
            var b = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions
            {
                CancellationToken = new CancellationToken(canceled: true),
                MaxNumberOfGroups = 1
            });

            Assert.NotNull(b.LinkTo(DataflowBlock.NullTarget<int[]>()));
            Assert.False(b.Targets[0].Post(42));
            Assert.False(b.Targets[1].Post(43));

            Task<bool> t1 = b.Targets[0].SendAsync(42);
            Task<bool> t2 = b.Targets[1].SendAsync(43);
            Assert.True(t1.IsCompleted);
            Assert.False(t1.Result);
            Assert.True(t2.IsCompleted);
            Assert.False(t2.Result);

            Assert.False(b.TryReceive(out int[] _));
            Assert.False(b.TryReceiveAll(out IList<int[]> _));
            Assert.Equal(expected: 0, actual: b.OutputCount);
            Assert.NotNull(b.Completion);
            b.Complete();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => b.Completion);
        }

        [Fact]
        public async Task TestCompletionThroughTargets()
        {
            for (int test = 1; test <= 2; test++)
            {
                var join = new JoinManyBlock<int>(2);
                switch (test)
                {
                    case 1:
                        join.Targets[0].Post(1);
                        join.Targets[0].Complete();
                        join.Targets[1].Complete();
                        break;
                    case 2:
                        join.Targets[0].Complete();
                        join.Targets[1].Post(1);
                        join.Targets[1].Complete();
                        break;
                }
                await join.Completion;
            }
        }

        [Fact]
        public async Task TestFaultThroughTargets()
        {
            var join2 = new JoinManyBlock<int>(2);
            join2.Targets[1].Fault(new FormatException());
            await Assert.ThrowsAsync<FormatException>(() => join2.Completion);
        }

        [Fact]
        public async Task TestCompletionTask()
        {
            await DataflowTestHelpers.TestCompletionTask(() => new JoinManyBlock<int>(2));

            await Assert.ThrowsAsync<NotSupportedException>(() => new JoinManyBlock<string>(2).Targets[0].Completion);
        }

        [Fact]
        public async Task TestCompletionThroughBlock()
        {
            var join2 = new JoinManyBlock<int>(2);
            join2.Targets[0].Post(1);
            join2.Complete();
            await join2.Completion;
        }

        [Fact]
        public async Task TestNonGreedyFailToConsumeReservedMessage()
        {
            var sources = Enumerable.Range(0, 2).Select(i => new DelegatePropagator<int, int>
            {
                ReserveMessageDelegate = delegate { return true; },
                ConsumeMessageDelegate = delegate (DataflowMessageHeader messageHeader, ITargetBlock<int> target, out bool messageConsumed)
                {
                    messageConsumed = false; // fail consumption of a message already reserved
                    Assert.Equal(expected: 0, actual: i); // shouldn't get to second source
                    return 0;
                }
            }).ToArray();

            var options = new GroupingDataflowBlockOptions { Greedy = false };
            JoinManyBlock<int> join = new JoinManyBlock<int>(2, options);

            join.Targets[0].OfferMessage(new DataflowMessageHeader(1), 0, sources[0], consumeToAccept: true); // call back ConsumeMassage
            join.Targets[1].OfferMessage(new DataflowMessageHeader(1), 0, sources[1], consumeToAccept: true); // call back ConsumeMassage

            await Assert.ThrowsAsync<InvalidOperationException>(() => join.Completion);
        }

        [Fact]
        public async Task TestNonGreedyDropPostponedOnCompletion()
        {
            var joinBlock = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });
            var source = new BufferBlock<int>();
            source.Post(1);
            source.LinkTo(joinBlock.Targets[0]);
            joinBlock.Complete();
            await joinBlock.Completion;
        }

        [Fact]
        public async Task TestNonGreedyReleasingFailsAtCompletion()
        {
            var joinBlock = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });
            var source = new DelegatePropagator<int, int>
            {
                ReserveMessageDelegate = (header, target) => true,
                ReleaseMessageDelegate = delegate { throw new FormatException(); }
            };

            joinBlock.Targets[0].OfferMessage(new DataflowMessageHeader(1), 1, source, consumeToAccept: true);
            joinBlock.Complete();

            await Assert.ThrowsAsync<FormatException>(() => joinBlock.Completion);
        }

        [Fact]
        public async Task TestNonGreedyConsumingFailsWhileJoining()
        {
            var joinBlock = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });
            var source1 = new DelegatePropagator<int, int>
            {
                ReserveMessageDelegate = (header, target) => true,
                ConsumeMessageDelegate = delegate (DataflowMessageHeader messageHeader, ITargetBlock<int> target, out bool messageConsumed)
                {
                    throw new FormatException();
                }
            };

            joinBlock.Targets[0].OfferMessage(new DataflowMessageHeader(1), 1, source1, consumeToAccept: true);

            var source2 = new BufferBlock<int>();
            source2.Post(2);
            source2.LinkTo(joinBlock.Targets[1]);

            await Assert.ThrowsAsync<FormatException>(() => joinBlock.Completion);
        }

        [Fact]
        public async Task TestNonGreedyPostponedMessagesNotAvailable()
        {
            var joinBlock = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });

            using var cts = new CancellationTokenSource();
            Task<bool>[] sends = Enumerable.Range(0, 3).Select(i => joinBlock.Targets[0].SendAsync(i, cts.Token)).ToArray();

            cts.Cancel();
            foreach (Task<bool> send in sends)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send);
            }

            joinBlock.Targets[1].Post(1);
            joinBlock.Targets[1].Complete();
            joinBlock.Targets[0].Complete();

            await joinBlock.Completion;
        }

        [Fact]
        public async Task TestMaxNumberOfGroups()
        {
            foreach (int boundedCapacity in new[] { DataflowBlockOptions.Unbounded, 2, 3 })
                foreach (bool greedy in DataflowTestHelpers.BooleanValues)
                {
                    var join = new JoinManyBlock<int>(2, new GroupingDataflowBlockOptions { MaxNumberOfGroups = 2, BoundedCapacity = 2 });
                    Task<bool>[] sends1 = Enumerable.Range(0, 10).Select(i => join.Targets[0].SendAsync(i)).ToArray();
                    Task<bool>[] sends2 = Enumerable.Range(0, 10).Select(i => join.Targets[1].SendAsync(i)).ToArray();
                    var ab = new ActionBlock<int[]>(i => { }, new ExecutionDataflowBlockOptions { BoundedCapacity = 1 });
                    join.LinkTo(ab, new DataflowLinkOptions { PropagateCompletion = true });
                    await join.Completion;
                    await Task.WhenAll(sends1);
                    await Task.WhenAll(sends2);
                }
        }

        [Fact]
        public async Task TestTree()
        {
            foreach (bool greedy in DataflowTestHelpers.BooleanValues)
                foreach (int boundedCapacity in new[] { DataflowBlockOptions.Unbounded, 1 })
                    foreach (int maxMessagesPerTask in new[] { DataflowBlockOptions.Unbounded, 1 })
                    {
                        var gdbo = new GroupingDataflowBlockOptions
                        {
                            Greedy = greedy,
                            BoundedCapacity = boundedCapacity,
                            MaxMessagesPerTask = maxMessagesPerTask
                        };
                        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

                        var join1 = new JoinManyBlock<int>(2, gdbo);
                        var join2 = new JoinManyBlock<int>(2, gdbo);
                        var join5 = new JoinManyBlock<int[]>(2, gdbo);

                        var join3 = new JoinManyBlock<int>(2, gdbo);
                        var join4 = new JoinManyBlock<int>(2, gdbo);
                        var join6 = new JoinManyBlock<int[]>(2, gdbo);

                        var join7 = new JoinManyBlock<int[][]>(2, gdbo);

                        int count = 0;
                        var sink = new ActionBlock<int[][][]>(i => count++);

                        join1.LinkTo(new ActionBlock<int[]>(item => { }), t => false); // ensure don't propagate across false filtered link
                        join1.LinkTo(join5.Targets[0], linkOptions, t => true); // ensure joins work through filters
                        join2.LinkTo(join5.Targets[1], linkOptions);
                        join3.LinkTo(join6.Targets[0], linkOptions, t => true);
                        join4.LinkTo(join6.Targets[1], linkOptions);
                        join5.LinkTo(join7.Targets[0], linkOptions, t => true);
                        join6.LinkTo(join7.Targets[1], linkOptions);
                        join7.LinkTo(sink, linkOptions);

                        const int Messages = 5;
                        CreateFillLink(Messages, join1.Targets[0]);
                        CreateFillLink(Messages, join1.Targets[1]);
                        CreateFillLink(Messages, join2.Targets[0]);
                        CreateFillLink(Messages, join2.Targets[1]);
                        CreateFillLink(Messages, join3.Targets[0]);
                        CreateFillLink(Messages, join3.Targets[1]);
                        CreateFillLink(Messages, join4.Targets[0]);
                        CreateFillLink(Messages, join4.Targets[1]);

                        await sink.Completion;
                        Assert.Equal(expected: Messages, actual: count);
                    }
        }

        private static void CreateFillLink<T>(int messages, ITargetBlock<T> target)
        {
            var b = new BufferBlock<T>();
            b.PostRange(0, messages, i => default);
            b.Complete();
            b.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });
        }

    }
}
