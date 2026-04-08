#include "part_thread_safe_state.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NCloud::NBlockStore::NStorage {

Y_UNIT_TEST_SUITE(TPartitionThreadSafeStateTest)
{
    Y_UNIT_TEST(ShouldReturnInvalidCommitIdWhenItOverflows)
    {
        TPartitionThreadSafeState state({}, 0, Max());

        UNIT_ASSERT(state.GenerateCommitId() == InvalidCommitId);
    }

    Y_UNIT_TEST(ShouldInitializeWithGenerationAndValue)
    {
        TPartitionThreadSafeState generator({}, 5, 10);
        ui64 expectedCommitId = MakeCommitId(5, 10);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateCommitId)
    {
        TPartitionThreadSafeState generator({}, 1, 0);

        ui64 commitId = generator.GenerateCommitId();
        ui64 expectedCommitId = MakeCommitId(1, 1);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateSequentialCommitIds)
    {
        TPartitionThreadSafeState generator({}, 2, 100);

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
        TPartitionThreadSafeState generator({}, 3, Max<ui32>());

        ui64 commitId = generator.GenerateCommitId();

        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeCommitId(3, Max<ui32>()),
            generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldHandleGenerationNearMaxValue)
    {
        TPartitionThreadSafeState generator({}, 10, Max<ui32>() - 1);

        ui64 commitId1 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(10, Max<ui32>()), commitId1);

        ui64 commitId2 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId2);
    }

    Y_UNIT_TEST(ShouldWorkWithZeroGeneration)
    {
        TPartitionThreadSafeState generator({}, 0, 0);

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(0, 1), commitId);
    }

    Y_UNIT_TEST(ShouldWorkWithMaxGeneration)
    {
        TPartitionThreadSafeState generator({}, Max<ui32>(), 0);

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(Max<ui32>(), 1), commitId);
    }

    Y_UNIT_TEST(ShouldPreserveGenerationInCommitId)
    {
        constexpr ui32 Generation = 12345;
        TPartitionThreadSafeState generator({}, Generation, 0);

        ui64 commitId = generator.GenerateCommitId();
        auto [gen, step] = ParseCommitId(commitId);

        UNIT_ASSERT_VALUES_EQUAL(Generation, gen);
        UNIT_ASSERT_VALUES_EQUAL(1, step);
    }

    Y_UNIT_TEST(ShouldBeThreadSafe)
    {
        TPartitionThreadSafeState generator({}, 1, 0);
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
