#include "disjoint_interval_builder.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Dump(const TVector<TIntervalPart>& parts)
{
    TStringBuilder out;
    out << "[";
    for (size_t i = 0; i < parts.size(); i++) {
        if (i != 0) {
            out << ", ";
        }
        out << "(";
        out << parts[i].Begin;
        out << ", ";
        out << parts[i].End;
        out << ", ";
        out << parts[i].Index;
        out << ")";
    }
    out << "]";
    return out;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDisjointIntervalBuilderTest)
{
    Y_UNIT_TEST(Empty)
    {
        TVector<TInterval> intervals;

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[]", Dump(res));
    }

    Y_UNIT_TEST(Simple)
    {
        TVector<TInterval> intervals = {{0, 10}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 10, 0)]", Dump(res));
    }

    Y_UNIT_TEST(TwoNonOverlapping)
    {
        TVector<TInterval> intervals = {{0, 10}, {20, 30}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 10, 0), (20, 30, 1)]", Dump(res));
    }

    Y_UNIT_TEST(TwoNonOverlappingReverseOrder)
    {
        TVector<TInterval> intervals = {{20, 30}, {0, 10}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 10, 1), (20, 30, 0)]", Dump(res));
    }

    Y_UNIT_TEST(TwoOverlapping)
    {
        TVector<TInterval> intervals = {{0, 20}, {10, 30}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 10, 0), (10, 30, 1)]", Dump(res));
    }

    Y_UNIT_TEST(TwoOverlappingReverseOrder)
    {
        TVector<TInterval> intervals = {{10, 30}, {0, 20}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 20, 1), (20, 30, 0)]", Dump(res));
    }

    Y_UNIT_TEST(Pyramids)
    {
        TVector<TInterval> intervals =
            {{0, 50}, {60, 90}, {10, 40}, {70, 80}, {20, 30}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL(
            "[(0, 10, 0), (10, 20, 2), (20, 30, 4), (30, 40, 2), (40, 50, 0), "
            "(60, 70, 1), (70, 80, 3), (80, 90, 1)]",
            Dump(res));
    }

    Y_UNIT_TEST(Adjacent)
    {
        TVector<TInterval> intervals = {{0, 10}, {20, 30}, {10, 20}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL(
            "[(0, 10, 0), (10, 20, 2), (20, 30, 1)]",
            Dump(res));
    }

    Y_UNIT_TEST(Same)
    {
        TVector<TInterval> intervals = {{0, 10}, {0, 10}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL("[(0, 10, 1)]", Dump(res));
    }

    Y_UNIT_TEST(Scenario1)
    {
        TVector<TInterval> intervals = {{11, 16}, {3, 14}, {0, 5}};

        auto res = BuildDisjointIntervalParts(intervals);

        UNIT_ASSERT_VALUES_EQUAL(
            "[(0, 5, 2), (5, 14, 1), (14, 16, 0)]",
            Dump(res));
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
