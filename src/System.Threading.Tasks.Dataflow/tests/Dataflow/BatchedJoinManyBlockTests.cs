// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Xunit;

namespace System.Threading.Tasks.Dataflow.Tests
{
    public class BatchedJoinManyBlockTests
    {
        [Fact]
        public void TestCtor()
        {
            var blocks2 = new[]
            {
                new BatchedJoinManyBlock<int>(1,2),
                new BatchedJoinManyBlock<int>(2,2, new GroupingDataflowBlockOptions {
                    MaxNumberOfGroups = 1 }),
                new BatchedJoinManyBlock<int>(3,2, new GroupingDataflowBlockOptions {
                    MaxMessagesPerTask = 1 }),
                new BatchedJoinManyBlock<int>(4,2, new GroupingDataflowBlockOptions {
                    MaxMessagesPerTask = 1, CancellationToken = new CancellationToken(true), MaxNumberOfGroups = 1 })
            };
            for (int i = 0; i < blocks2.Length; i++)
            {
                Assert.Equal(expected: i + 1, actual: blocks2[i].BatchSize);
                Assert.Equal(expected: 0, actual: blocks2[i].OutputCount);
                Assert.NotNull(blocks2[i].Completion);
            }
        }

        [Fact]
        public void TestArgumentExceptions()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new BatchedJoinManyBlock<int>(0, 2));
            Assert.Throws<ArgumentOutOfRangeException>(() => new BatchedJoinManyBlock<int>(1, 0));
            Assert.Throws<ArgumentNullException>(() => new BatchedJoinManyBlock<int>(2, 2, null));
            AssertExtensions.Throws<ArgumentException>("dataflowBlockOptions", () => new BatchedJoinManyBlock<int>(2, 2, new GroupingDataflowBlockOptions { Greedy = false }));
            AssertExtensions.Throws<ArgumentException>("dataflowBlockOptions", () => new BatchedJoinManyBlock<int>(2, 2, new GroupingDataflowBlockOptions { BoundedCapacity = 2 }));
            Assert.Throws<ArgumentNullException>(() => ((IDataflowBlock)new BatchedJoinManyBlock<int>(2, 2)).Fault(null));
            Assert.Throws<ArgumentNullException>(() => new BatchedJoinManyBlock<int>(2, 2).Targets[0].Fault(null));

            DataflowTestHelpers.TestArgumentsExceptions(new BatchedJoinManyBlock<int>(1, 2));
        }

        [Fact]
        public void TestToString()
        {
            DataflowTestHelpers.TestToString(nameFormat =>
                nameFormat != null ?
                    new BatchedJoinManyBlock<int>(2, 2, new GroupingDataflowBlockOptions() { NameFormat = nameFormat }) :
                    new BatchedJoinManyBlock<int>(2, 2));
        }

        [Fact]
        public async Task TestCompletionTask()
        {
            await DataflowTestHelpers.TestCompletionTask(() => new BatchedJoinManyBlock<int>(2, 2));

            await Assert.ThrowsAsync<NotSupportedException>(() => new BatchedJoinManyBlock<int>(2, 2).Targets[0].Completion);
        }

        [Fact]
        public void TestPostThenReceive2()
        {
            const int Iters = 10;
            var block = new BatchedJoinManyBlock<int>(2, 2);
            for (int i = 0; i < Iters; i++)
            {
                int prevCount = block.OutputCount;
                block.Targets[0].Post(i);
                Assert.Equal(expected: prevCount, actual: block.OutputCount);
                block.Targets[1].Post(i);

                if (i % block.BatchSize == 0)
                {
                    Assert.Equal(expected: prevCount + 1, actual: block.OutputCount);

                    Assert.False(block.TryReceive(f => false, out IList<int>[] msg));
                    Assert.True(block.TryReceive(out msg));

                    Assert.Equal(expected: 1, actual: msg[0].Count);
                    Assert.Equal(expected: 1, actual: msg[1].Count);

                    for (int j = 0; j < msg[0].Count; j++)
                    {
                        Assert.Equal(msg[0][j], msg[1][j]);
                    }
                }
            }
        }

        [Fact]
        public void TestPostAllThenReceive()
        {
            const int Iters = 10;

            var block = new BatchedJoinManyBlock<int>(2, 2);
            for (int i = 0; i < Iters; i++)
            {
                block.Targets[0].Post(i);
                block.Targets[1].Post(i);
            }
            Assert.Equal(expected: Iters, actual: block.OutputCount);

            for (int i = 0; i < block.OutputCount; i++)
            {
                Assert.True(block.TryReceive(out IList<int>[] msg));

                Assert.Equal(expected: 1, actual: msg[0].Count);
                Assert.Equal(expected: 1, actual: msg[1].Count);

                for (int j = 0; j < msg[0].Count; j++)
                {
                    Assert.Equal(msg[0][j], msg[1][j]);
                }
            }
        }

        [Fact]
        public void TestUnbalanced2()
        {
            const int Iters = 10, NumBatches = 2;
            int batchSize = Iters / NumBatches;

            var block = new BatchedJoinManyBlock<int>(batchSize, 2);
            for (int i = 0; i < Iters; i++)
            {
                block.Targets[1].Post(i);
                Assert.Equal(expected: (i + 1) / batchSize, actual: block.OutputCount);
            }

            IList<IList<int>[]> items;
            Assert.True(block.TryReceiveAll(out items));
            Assert.Equal(expected: NumBatches, actual: items.Count);

            for (int i = 0; i < items.Count; i++)
            {
                var item = items[i];
                Assert.NotNull(item[0]);
                Assert.NotNull(item[1]);
                Assert.Equal(expected: batchSize, actual: item[1].Count);
                for (int j = 0; j < batchSize; j++)
                {
                    Assert.Equal(expected: (i * batchSize) + j, actual: item[1][j]);
                }
            }
            Assert.False(block.TryReceiveAll(out items));
        }

        [Fact]
        public void TestCompletion()
        {
            const int Iters = 10;

            var block = new BatchedJoinManyBlock<int>(2, 2);
            for (int i = 0; i < Iters; i++)
            {
                block.Targets[0].Post(i);
                block.Targets[1].Post(i);
            }
            Assert.Equal(expected: Iters, actual: block.OutputCount);

            block.Targets[0].Post(10);
            block.Targets[0].Complete();
            block.Targets[1].Complete();
            Assert.Equal(expected: Iters + 1, actual: block.OutputCount);

            IList<int>[] item;
            for (int i = 0; i < Iters; i++)
            {
                Assert.True(block.TryReceive(out item));
                Assert.Equal(expected: 1, actual: item[0].Count);
                Assert.Equal(expected: 1, actual: item[1].Count);
            }

            Assert.True(block.TryReceive(out item));
            Assert.Equal(expected: 1, actual: item[0].Count);
            Assert.Equal(expected: 0, actual: item[1].Count);
        }

        [Fact]
        public async Task TestPrecanceled2()
        {
            var b = new BatchedJoinManyBlock<int>(42, 2,
                new GroupingDataflowBlockOptions { CancellationToken = new CancellationToken(canceled: true), MaxNumberOfGroups = 1 });

            Assert.NotNull(b.LinkTo(new ActionBlock<IList<int>[]>(delegate { })));
            Assert.False(b.Targets[0].Post(42));
            Assert.False(b.Targets[1].Post(42));

            foreach (var target in new[] { b.Targets[0], b.Targets[1] })
            {
                var t = target.SendAsync(42);
                Assert.True(t.IsCompleted);
                Assert.False(t.Result);
            }

            Assert.False(b.TryReceiveAll(out IList<IList<int>[]> ignoredValues));
            Assert.False(b.TryReceive(out IList<int>[] ignoredValue));
            Assert.Equal(expected: 0, actual: b.OutputCount);
            Assert.NotNull(b.Completion);
            b.Targets[0].Complete();
            b.Targets[1].Complete();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => b.Completion);
        }

        [Fact]
        public async Task TestCompletesThroughTargets()
        {
            var b2 = new BatchedJoinManyBlock<int>(99, 2);
            b2.Targets[0].Post(1);
            b2.Targets[0].Complete();
            b2.Targets[1].Complete();
            IList<int>[] item2 = await b2.ReceiveAsync();
            Assert.Equal(expected: 1, actual: item2[0].Count);
            Assert.Equal(expected: 0, actual: item2[1].Count);
            await b2.Completion;
        }

        [Fact]
        public async Task TestFaultsThroughTargets()
        {
            var b2 = new BatchedJoinManyBlock<int>(99, 2);
            b2.Targets[0].Post(1);
            ((IDataflowBlock)b2.Targets[0]).Fault(new FormatException());
            await Assert.ThrowsAsync<FormatException>(() => b2.Completion);
        }

        [Fact]
        public async Task TestCompletesThroughBlock()
        {
            var b2 = new BatchedJoinManyBlock<int>(99, 2);
            b2.Targets[0].Post(1);
            b2.Complete();
            IList<int>[] item2 = await b2.ReceiveAsync();
            Assert.Equal(expected: 1, actual: item2[0].Count);
            Assert.Equal(expected: 0, actual: item2[1].Count);
            await b2.Completion;
        }

        [Fact]
        public async Task TestReserveReleaseConsume()
        {
            var b2 = new BatchedJoinManyBlock<int>(2, 2);
            b2.Targets[0].Post(1);
            b2.Targets[1].Post(2);
            await DataflowTestHelpers.TestReserveAndRelease(b2);

            b2 = new BatchedJoinManyBlock<int>(2, 2);
            b2.Targets[0].Post(1);
            b2.Targets[1].Post(2);
            await DataflowTestHelpers.TestReserveAndConsume(b2);
        }

        [Fact]
        public async Task TestConsumeToAccept()
        {
            var wob = new WriteOnceBlock<int>(i => i * 2);
            wob.Post(1);
            await wob.Completion;

            var b2 = new BatchedJoinManyBlock<int>(1, 2);
            wob.LinkTo(b2.Targets[1], new DataflowLinkOptions { PropagateCompletion = true });
            IList<int>[] item2 = await b2.ReceiveAsync();
            Assert.Equal(expected: 0, actual: item2[0].Count);
            Assert.Equal(expected: 1, actual: item2[1].Count);
            b2.Targets[0].Complete();

            await b2.Completion;
        }

        [Fact]
        public async Task TestOfferMessage2()
        {
            Func<ITargetBlock<int>> generator = () =>
            {
                var b = new BatchedJoinManyBlock<int>(1, 2);
                return b.Targets[0];
            };
            DataflowTestHelpers.TestOfferMessage_ArgumentValidation(generator());
            DataflowTestHelpers.TestOfferMessage_AcceptsDataDirectly(generator());
            await DataflowTestHelpers.TestOfferMessage_AcceptsViaLinking(generator());
        }

        [Fact]
        public async Task TestMaxNumberOfGroups()
        {
            const int MaxGroups = 2;

            var b2 = new BatchedJoinManyBlock<int>(1, 2, new GroupingDataflowBlockOptions { MaxNumberOfGroups = MaxGroups });
            b2.Targets[0].PostRange(0, MaxGroups);
            Assert.False(b2.Targets[0].Post(42));
            Assert.False(b2.Targets[1].Post(42));
            IList<IList<int>[]> items2;
            Assert.True(b2.TryReceiveAll(out items2));
            Assert.Equal(expected: MaxGroups, actual: items2.Count);
            await b2.Completion;
        }
    }
}
