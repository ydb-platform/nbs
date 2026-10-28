#include "part_thread_safe_state.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/core/executor_thread.h>
#include <contrib/ydb/library/actors/core/mailbox.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <barrier>
#include <thread>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTimestampTestRuntime: public NActors::TTestActorRuntimeBase
{
private:
    NActors::TMailbox Mailbox;

public:
    TTimestampTestRuntime()
    {
        Initialize();
    }

    NActors::TActorContext GetContext()
    {
        return NActors::TActorContext(
            Mailbox,
            *GetNodeById(GetNodeId())->ExecutorThread,
            0,
            {});
    }

    void SetClocks(TInstant timestamp, NActors::TMonotonic monotonic)
    {
        auto* node = GetNodeById(GetNodeId());
        AtomicStore(node->ActorSystemTimestamp, timestamp.MicroSeconds());
        AtomicStore(node->ActorSystemMonotonic, monotonic.MicroSeconds());
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TPartitionThreadSafeStateTest)
{
    Y_UNIT_TEST(ShouldAdvanceFreshWriteTimestampWithMonotonicTime)
    {
        TTimestampTestRuntime runtime;
        const auto ctx = runtime.GetContext();
        TPartitionThreadSafeState state;
        state.Init({}, 1, 0, TInstant::Seconds(100), TMonotonic::Seconds(10));

        {
            // Timestamp reads must work with StateLock held.
            auto guard = state.AccessTrimFreshLogBarriers();
            runtime.SetClocks(TInstant::Seconds(100), TMonotonic::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(100), state.GetFreshWriteTimestamp(ctx));

            runtime.SetClocks(TInstant::Seconds(50), TMonotonic::Seconds(11));
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(101), state.GetFreshWriteTimestamp(ctx));

            runtime.SetClocks(TInstant::Seconds(1000), TMonotonic::Seconds(12));
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(102), state.GetFreshWriteTimestamp(ctx));
        }
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(1, 0), state.GetLastCommitId());

        const auto firstWrite = state.StartFreshWrite(ctx, 1);
        runtime.SetClocks(TInstant::Seconds(20), TMonotonic::Seconds(13));
        const auto secondWrite = state.StartFreshWrite(ctx, 1);
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(1, 1), firstWrite.CommitId);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(102), firstWrite.Timestamp);
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(1, 2), secondWrite.CommitId);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(103), secondWrite.Timestamp);
    }

    Y_UNIT_TEST(ShouldReadFreshWriteTimestampConcurrently)
    {
        TTimestampTestRuntime runtime;
        runtime.SetClocks(TInstant::Seconds(100), TMonotonic::Seconds(10));
        const auto ctx = runtime.GetContext();
        TPartitionThreadSafeState state;
        state.Init({}, 1, 0, TInstant::Seconds(100), TMonotonic::Seconds(10));

        constexpr size_t NumThreads = 10;
        constexpr size_t RequestsPerThread = 1000;
        std::barrier start(NumThreads);
        std::vector<std::thread> threads;
        std::vector<std::vector<TInstant>> timestamps(NumThreads);
        for (size_t i = 0; i < NumThreads; ++i) {
            threads.emplace_back(
                [&, i]
                {
                    timestamps[i].reserve(RequestsPerThread);
                    start.arrive_and_wait();
                    for (size_t j = 0; j < RequestsPerThread; ++j) {
                        timestamps[i].push_back(
                            i % 2 ? state.StartFreshWrite(ctx, 1).Timestamp
                                  : state.GetFreshWriteTimestamp(ctx));
                    }
                });
        }
        for (auto& thread: threads) {
            thread.join();
        }
        for (const auto& values: timestamps) {
            for (const auto timestamp: values) {
                UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100), timestamp);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            MakeCommitId(1, NumThreads / 2 * RequestsPerThread),
            state.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldTakeFreshTrimSnapshot)
    {
        TTimestampTestRuntime runtime;
        const auto ctx = runtime.GetContext();
        TPartitionThreadSafeState state;
        state.Init({}, 1, 0, TInstant::Seconds(100), TMonotonic::Seconds(10));

        auto snapshot = state.GetFreshTrimSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(1, 0), snapshot.PrefixCommitId);
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(1, 0), snapshot.SafeCommitId);
        UNIT_ASSERT(!snapshot.LiveCommitIds);

        const ui64 first = state.StartFreshWrite(ctx, 1).CommitId;
        const ui64 second = state.StartFreshWrite(ctx, 2).CommitId;
        const ui64 third = state.StartFreshWrite(ctx, 1).CommitId;

        // Finished writes keep their trim barrier references until their
        // blocks are overwritten or flushed.
        state.FinishFreshWrite(ctx, first, 1, false);
        state.FinishFreshWrite(ctx, third, 1, false);

        // S stays below the write in flight.
        snapshot = state.GetFreshTrimSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(first - 1, snapshot.PrefixCommitId);
        UNIT_ASSERT_VALUES_EQUAL(second - 1, snapshot.SafeCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({first}),
            snapshot.LiveCommitIds);

        state.FinishFreshWrite(ctx, second, 2, false);

        snapshot = state.GetFreshTrimSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(first - 1, snapshot.PrefixCommitId);
        UNIT_ASSERT_VALUES_EQUAL(third, snapshot.SafeCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({first, second, third}),
            snapshot.LiveCommitIds);

        // LiveCommitIds lists only (P, S].
        state.AccessTrimFreshLogBarriers()->ReleaseBarrier(first);
        snapshot = state.GetFreshTrimSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(second - 1, snapshot.PrefixCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({second, third}),
            snapshot.LiveCommitIds);

        // A failed write releases its references.
        const ui64 fourth = state.StartFreshWrite(ctx, 1).CommitId;
        state.FinishFreshWrite(ctx, fourth, 1, true);
        snapshot = state.GetFreshTrimSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(fourth, snapshot.SafeCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({second, third}),
            snapshot.LiveCommitIds);
    }

    Y_UNIT_TEST(ShouldReturnInvalidCommitIdWhenItOverflows)
    {
        TPartitionThreadSafeState state;
        state.Init({}, 0, Max(), {}, {});

        UNIT_ASSERT(state.GenerateCommitId() == InvalidCommitId);
    }

    Y_UNIT_TEST(ShouldInitializeWithGenerationAndValue)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 5, 10, {}, {});
        ui64 expectedCommitId = MakeCommitId(5, 10);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateCommitId)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 1, 0, {}, {});

        ui64 commitId = generator.GenerateCommitId();
        ui64 expectedCommitId = MakeCommitId(1, 1);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateSequentialCommitIds)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 2, 100, {}, {});

        ui64 commitId1 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(2, 101), commitId1);

        ui64 commitId2 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(2, 102), commitId2);

        ui64 commitId3 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(2, 103), commitId3);

        UNIT_ASSERT_VALUES_EQUAL(
            MakeCommitId(2, 103),
            generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldReturnInvalidCommitIdWhenOverflows)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 3, Max<ui32>(), {}, {});

        ui64 commitId = generator.GenerateCommitId();

        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeCommitId(3, Max<ui32>()),
            generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldHandleGenerationNearMaxValue)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 10, Max<ui32>() - 1, {}, {});

        ui64 commitId1 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(10, Max<ui32>()), commitId1);

        ui64 commitId2 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId2);
    }

    Y_UNIT_TEST(ShouldWorkWithZeroGeneration)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 0, 0, {}, {});

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(0, 1), commitId);
    }

    Y_UNIT_TEST(ShouldWorkWithMaxGeneration)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, Max<ui32>(), 0, {}, {});

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(Max<ui32>(), 1), commitId);
    }

    Y_UNIT_TEST(ShouldPreserveGenerationInCommitId)
    {
        constexpr ui32 Generation = 12345;
        TPartitionThreadSafeState generator;
        generator.Init({}, Generation, 0, {}, {});

        ui64 commitId = generator.GenerateCommitId();
        auto [gen, step] = ParseCommitId(commitId);

        UNIT_ASSERT_VALUES_EQUAL(Generation, gen);
        UNIT_ASSERT_VALUES_EQUAL(1, step);
    }

    Y_UNIT_TEST(ShouldBeThreadSafe)
    {
        TPartitionThreadSafeState generator;
        generator.Init({}, 1, 0, {}, {});
        constexpr size_t NumThreads = 10;
        constexpr size_t GenerationsPerThread = 1000;

        std::vector<std::thread> threads;
        std::vector<std::vector<ui64>> generatedIds(NumThreads);

        threads.reserve(NumThreads);

        for (size_t i = 0; i < NumThreads; ++i) {
            threads.emplace_back(
                [&generator, &generatedIds, i]()
                {
                    generatedIds[i].reserve(GenerationsPerThread);
                    for (size_t j = 0; j < GenerationsPerThread; ++j) {
                        ui64 commitId = generator.GenerateCommitId();
                        generatedIds[i].push_back(commitId);
                    }
                });
        }

        for (auto& thread: threads) {
            thread.join();
        }

        // Collect all generated IDs
        std::vector<ui64> allIds;
        allIds.reserve(NumThreads * GenerationsPerThread);
        for (const auto& threadIds: generatedIds) {
            allIds.insert(allIds.end(), threadIds.begin(), threadIds.end());
        }

        // Check that all IDs are unique
        std::ranges::sort(allIds);
        auto unique = std::ranges::unique(allIds);
        UNIT_ASSERT_VALUES_EQUAL(
            NumThreads * GenerationsPerThread,
            std::distance(allIds.begin(), unique.begin()));

        // Check that the last commit ID is correct
        ui64 expectedLastCommitId =
            MakeCommitId(1, NumThreads * GenerationsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLastCommitId,
            generator.GetLastCommitId());
    }
}
}   // namespace NCloud::NBlockStore::NStorage
