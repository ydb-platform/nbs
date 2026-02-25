#include "commit_id_generator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <thread>
#include <vector>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCounterTest)
{
    Y_UNIT_TEST(ShouldInitializeWithValue)
    {
        TCounter counter(42);
        UNIT_ASSERT_VALUES_EQUAL(42, counter.GetValue());
    }

    Y_UNIT_TEST(ShouldIncrementValue)
    {
        TCounter counter(0);
        auto result = counter.SafeIncrement();

        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(1, *result);
        UNIT_ASSERT_VALUES_EQUAL(1, counter.GetValue());
    }

    Y_UNIT_TEST(ShouldIncrementMultipleTimes)
    {
        TCounter counter(10);

        auto result1 = counter.SafeIncrement();
        UNIT_ASSERT(result1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(11, *result1);

        auto result2 = counter.SafeIncrement();
        UNIT_ASSERT(result2.has_value());
        UNIT_ASSERT_VALUES_EQUAL(12, *result2);

        auto result3 = counter.SafeIncrement();
        UNIT_ASSERT(result3.has_value());
        UNIT_ASSERT_VALUES_EQUAL(13, *result3);

        UNIT_ASSERT_VALUES_EQUAL(13, counter.GetValue());
    }

    Y_UNIT_TEST(ShouldReturnEmptyOptionalWhenOverflows)
    {
        TCounter counter(Max<ui32>());
        auto result = counter.SafeIncrement();

        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), counter.GetValue());
    }

    Y_UNIT_TEST(ShouldHandleIncrementNearMaxValue)
    {
        TCounter counter(Max<ui32>() - 2);

        auto result1 = counter.SafeIncrement();
        UNIT_ASSERT(result1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>() - 1, *result1);

        auto result2 = counter.SafeIncrement();
        UNIT_ASSERT(result2.has_value());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), *result2);

        auto result3 = counter.SafeIncrement();
        UNIT_ASSERT(!result3.has_value());

        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), counter.GetValue());
    }

    Y_UNIT_TEST(ShouldBeThreadSafe)
    {
        TCounter counter(0);
        constexpr size_t NumThreads = 10;
        constexpr size_t IncrementsPerThread = 1000;

        std::vector<std::thread> threads;
        threads.reserve(NumThreads);

        for (size_t i = 0; i < NumThreads; ++i) {
            threads.emplace_back(
                [&counter]()
                {
                    for (size_t j = 0; j < IncrementsPerThread; ++j) {
                        counter.SafeIncrement();
                    }
                });
        }

        for (auto& thread: threads) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            NumThreads * IncrementsPerThread,
            counter.GetValue());
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommitIdGeneratorTest)
{
    Y_UNIT_TEST(ShouldInitializeWithGenerationAndValue)
    {
        TCommitIdGenerator generator(5, 10);
        ui64 expectedCommitId = MakeCommitId(5, 10);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateCommitId)
    {
        TCommitIdGenerator generator(1, 0);

        ui64 commitId = generator.GenerateCommitId();
        ui64 expectedCommitId = MakeCommitId(1, 1);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(expectedCommitId, generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldGenerateSequentialCommitIds)
    {
        TCommitIdGenerator generator(2, 100);

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
        TCommitIdGenerator generator(3, Max<ui32>());

        ui64 commitId = generator.GenerateCommitId();

        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeCommitId(3, Max<ui32>()),
            generator.GetLastCommitId());
    }

    Y_UNIT_TEST(ShouldHandleGenerationNearMaxValue)
    {
        TCommitIdGenerator generator(10, Max<ui32>() - 1);

        ui64 commitId1 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(10, Max<ui32>()), commitId1);

        ui64 commitId2 = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, commitId2);
    }

    Y_UNIT_TEST(ShouldWorkWithZeroGeneration)
    {
        TCommitIdGenerator generator(0, 0);

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(0, 1), commitId);
    }

    Y_UNIT_TEST(ShouldWorkWithMaxGeneration)
    {
        TCommitIdGenerator generator(Max<ui32>(), 0);

        ui64 commitId = generator.GenerateCommitId();
        UNIT_ASSERT_VALUES_EQUAL(MakeCommitId(Max<ui32>(), 1), commitId);
    }

    Y_UNIT_TEST(ShouldPreserveGenerationInCommitId)
    {
        constexpr ui32 Generation = 12345;
        TCommitIdGenerator generator(Generation, 0);

        ui64 commitId = generator.GenerateCommitId();
        auto [gen, step] = ParseCommitId(commitId);

        UNIT_ASSERT_VALUES_EQUAL(Generation, gen);
        UNIT_ASSERT_VALUES_EQUAL(1, step);
    }

    Y_UNIT_TEST(ShouldBeThreadSafe)
    {
        TCommitIdGenerator generator(1, 0);
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
