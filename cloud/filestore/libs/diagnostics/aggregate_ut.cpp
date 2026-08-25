#include "aggregate.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NAggregation{

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TValue
{
    ui64 Value = 0;

    void Add(const TValue& other)
    {
        Value += other.Value;
    }
};

void AssertAggregate(
    const TVector<NAggregation::TResult<TValue>>& results,
    const TVector<TString>& labels,
    ui64 expectedValue)
{
    size_t matches = 0;
    for (const auto& result: results) {
        if (result.Labels == labels) {
            ++matches;
            UNIT_ASSERT_VALUES_EQUAL(
                expectedValue,
                result.GroupAggregate.Value);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(1, matches);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAggregationTest)
{
    Y_UNIT_TEST(ShouldReturnEmptyResultForEmptyInput)
    {
        TVector<NAggregation::TRow<TValue>> rows;

        const auto results = NAggregation::Aggregate(rows);

        UNIT_ASSERT(results.empty());
    }

    Y_UNIT_TEST(ShouldAggregateEveryCombinationOfLabels)
    {
        TVector<NAggregation::TRow<TValue>> rows = {
            {{"10", "shard-a", "Read"}, {1}},
            {{"10", "shard-b", "Read"}, {2}},
            {{"20", "shard-a", "Write"}, {4}}};

        const auto results = NAggregation::Aggregate(rows);

        UNIT_ASSERT_VALUES_EQUAL(18, results.size());
        for (const auto& result: results) {
            UNIT_ASSERT_VALUES_EQUAL(3, result.Labels.size());
        }

        AssertAggregate(results, {"", "", ""}, 7);
        AssertAggregate(results, {"10", "", ""}, 3);
        AssertAggregate(results, {"", "shard-a", ""}, 5);
        AssertAggregate(results, {"", "", "Read"}, 3);
        AssertAggregate(results, {"10", "shard-a", ""}, 1);
        AssertAggregate(results, {"10", "", "Read"}, 3);
        AssertAggregate(results, {"", "shard-a", "Write"}, 4);
        AssertAggregate(results, {"20", "shard-a", "Write"}, 4);
    }
}

}   // namespace NCloud::NFileStore::NAggregation
