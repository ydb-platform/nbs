#include "ranking.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestValue
{
    ui64 Key = 0;
    int Score = 0;
};

struct TTestComparator
{
    bool operator()(const TTestValue& lhs, const TTestValue& rhs) const
    {
        // Score ASC, Key ASC
        return std::tie(lhs.Score, lhs.Key) < std::tie(rhs.Score, rhs.Key);
    }
};

struct TFaultyTestComparator
{
    bool operator()(const TTestValue& lhs, const TTestValue& rhs) const
    {
        // Score ASC, no tiebreak on Key
        return std::tie(lhs.Score) < std::tie(rhs.Score);
    }
};

struct TTestKeyExtractor
{
    ui64 operator()(const TTestValue& value) const
    {
        return value.Key;
    }
};

using TTestRanking =
    TBoundedRanking<ui64, TTestValue, TTestComparator, TTestKeyExtractor>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBoundedRankingTest)
{
    Y_UNIT_TEST(ShouldInsertAndFind)
    {
        TTestRanking ranking(3, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});

        const auto* value = ranking.Find(1);

        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(1, value->Key);
        UNIT_ASSERT_VALUES_EQUAL(10, value->Score);
    }

    Y_UNIT_TEST(ShouldReturnNullForMissingKey)
    {
        TTestRanking ranking(3, TTestComparator{}, TTestKeyExtractor{});

        UNIT_ASSERT(!ranking.Find(42));
    }

    Y_UNIT_TEST(ShouldReplaceExistingValue)
    {
        TTestRanking ranking(3, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        ranking.InsertOrUpdate({1, 20});

        const auto values = ranking.GetNLast(10);

        UNIT_ASSERT_VALUES_EQUAL(1, values.size());
        UNIT_ASSERT_VALUES_EQUAL(1, values[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(20, values[0].Score);

        const auto* value = ranking.Find(1);
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(20, value->Score);
    }

    Y_UNIT_TEST(ShouldReturnHighestRankedValuesFirst)
    {
        TTestRanking ranking(5, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        ranking.InsertOrUpdate({2, 30});
        ranking.InsertOrUpdate({3, 20});

        const auto values = ranking.GetNLast(3);

        UNIT_ASSERT_VALUES_EQUAL(3, values.size());
        UNIT_ASSERT_VALUES_EQUAL(2, values[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(3, values[1].Key);
        UNIT_ASSERT_VALUES_EQUAL(1, values[2].Key);
    }

    Y_UNIT_TEST(ShouldLimitNumberOfReturnedValues)
    {
        TTestRanking ranking(5, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        ranking.InsertOrUpdate({2, 30});
        ranking.InsertOrUpdate({3, 20});

        const auto values = ranking.GetNLast(2);

        UNIT_ASSERT_VALUES_EQUAL(2, values.size());
        UNIT_ASSERT_VALUES_EQUAL(2, values[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(3, values[1].Key);
    }

    Y_UNIT_TEST(ShouldEvictLowestRankedValue)
    {
        TTestRanking ranking(2, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        ranking.InsertOrUpdate({2, 30});
        ranking.InsertOrUpdate({3, 20});

        const auto values = ranking.GetNLast(10);

        UNIT_ASSERT_VALUES_EQUAL(2, values.size());
        UNIT_ASSERT_VALUES_EQUAL(2, values[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(3, values[1].Key);

        UNIT_ASSERT(!ranking.Find(1));
        UNIT_ASSERT(ranking.Find(2));
        UNIT_ASSERT(ranking.Find(3));
    }

    Y_UNIT_TEST(ShouldUseKeyAsTieBreaker)
    {
        TTestRanking ranking(3, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        ranking.InsertOrUpdate({2, 10});

        const auto values = ranking.GetNLast(10);

        UNIT_ASSERT_VALUES_EQUAL(2, values.size());

        UNIT_ASSERT_VALUES_EQUAL(2, values[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(1, values[1].Key);
    }

    Y_UNIT_TEST(ShouldRejectDuplicateInserts)
    {
        TTestRanking ranking(1, TFaultyTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});
        UNIT_ASSERT_VALUES_EQUAL(false, ranking.InsertOrUpdate({2, 10}));

    }

    Y_UNIT_TEST(ShouldContainNoValuesWhenMaxEntriesIsZero)
    {
        TTestRanking ranking(0, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});

        UNIT_ASSERT_VALUES_EQUAL(0, ranking.GetNLast(10).size());
        UNIT_ASSERT(!ranking.Find(1));
    }

    Y_UNIT_TEST(ShouldReturnEmptyWhenRequestedCountIsZero)
    {
        TTestRanking ranking(3, TTestComparator{}, TTestKeyExtractor{});

        ranking.InsertOrUpdate({1, 10});

        UNIT_ASSERT_VALUES_EQUAL(0, ranking.GetNLast(0).size());
    }
}

}   // namespace NCloud::NFileStore::NStorage
