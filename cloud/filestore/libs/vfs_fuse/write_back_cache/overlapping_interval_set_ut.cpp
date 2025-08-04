#include "overlapping_interval_set.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

namespace {

struct TInterval
{
    ui64 Begin = 0;
    ui64 End = 0;
};

TInterval GenerateRandomInterval(ui64 intervalSize)
{
    auto a = RandomNumber(intervalSize);
    auto b = RandomNumber(intervalSize);

    if (a > b) {
        std::swap(a, b);
    }

    return {.Begin = a, .End = b + 1};
}

TString Dump(const TVector<TInterval>& intervalSet)
{
    TStringBuilder out;
    out << "[";
    for (size_t i = 0; i < intervalSet.size(); i++) {
        if (i != 0) {
            out << ", ";
        }
        out << "(";
        out << intervalSet[i].Begin;
        out << ", ";
        out << intervalSet[i].End;
        out << ")";
    }
    out << "]";
    return out;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOverlappedIntervalTest)
{
    Y_UNIT_TEST(AddAndRemoveSingleInterval)
    {
        TOverlappingIntervalSet set;

        auto check = [&](bool expected) {
            UNIT_ASSERT(!set.HasIntersection(0, 1));
            UNIT_ASSERT(!set.HasIntersection(0, 2));

            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(0, 3));
            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(0, 5));
            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(0, 7));

            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(2, 3));
            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(2, 5));
            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(2, 7));

            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(4, 5));
            UNIT_ASSERT_EQUAL(expected, set.HasIntersection(4, 7));

            UNIT_ASSERT(!set.HasIntersection(5, 7));
            UNIT_ASSERT(!set.HasIntersection(6, 7));
        };

        check(false);

        set.AddInterval(2, 5);
        check(true);

        set.RemoveInterval(2, 5);
        check(false);
    }

    Y_UNIT_TEST(AddAndRemoveDuplicateInterval)
    {
        TOverlappingIntervalSet set;

        set.AddInterval(2, 5);
        set.AddInterval(2, 5);

        UNIT_ASSERT(set.HasIntersection(2, 5));

        set.RemoveInterval(2, 5);

        UNIT_ASSERT(set.HasIntersection(2, 5));

        set.RemoveInterval(2, 5);

        UNIT_ASSERT(!set.HasIntersection(2, 5));
    }

    Y_UNIT_TEST(AddAndRemoveTwoIntervals)
    {
        TOverlappingIntervalSet set;

        set.AddInterval(2, 4);
        set.AddInterval(5, 7);

        UNIT_ASSERT(set.HasIntersection(2, 4));
        UNIT_ASSERT(!set.HasIntersection(4, 5));
        UNIT_ASSERT(set.HasIntersection(5, 7));

        set.RemoveInterval(2, 4);

        UNIT_ASSERT(!set.HasIntersection(2, 4));
        UNIT_ASSERT(!set.HasIntersection(4, 5));
        UNIT_ASSERT(set.HasIntersection(5, 7));

        set.RemoveInterval(5, 7);

        UNIT_ASSERT(!set.HasIntersection(2, 4));
        UNIT_ASSERT(!set.HasIntersection(4, 5));
        UNIT_ASSERT(!set.HasIntersection(5, 7));
    }

    Y_UNIT_TEST(AddAndRemoveTwoOverlappingIntervals)
    {
        TOverlappingIntervalSet set;

        set.AddInterval(2, 5);
        set.AddInterval(4, 7);

        UNIT_ASSERT(set.HasIntersection(2, 4));
        UNIT_ASSERT(set.HasIntersection(4, 5));
        UNIT_ASSERT(set.HasIntersection(5, 7));

        set.RemoveInterval(2, 5);

        UNIT_ASSERT(!set.HasIntersection(2, 4));
        UNIT_ASSERT(set.HasIntersection(4, 5));
        UNIT_ASSERT(set.HasIntersection(5, 7));

        set.RemoveInterval(4, 7);

        UNIT_ASSERT(!set.HasIntersection(2, 4));
        UNIT_ASSERT(!set.HasIntersection(4, 5));
        UNIT_ASSERT(!set.HasIntersection(5, 7));
    }

    void ValidateOverlappingIntervalSet(
        const TOverlappingIntervalSet& set,
        const TVector<TInterval>& intervalSet,
        TVector<int>& expectedData)
    {
        for (ui64 begin = 0; begin < expectedData.size(); begin++) {
            int sum = 0;
            for (ui64 end = begin + 1; end <= expectedData.size(); end++) {
                sum += expectedData[end - 1];
                auto actual = set.HasIntersection(begin, end);

                UNIT_ASSERT_C((sum != 0) == actual,
                    "Invalid HasIntersection(" << begin << ", " << end << ")"
                    " result for " << Dump(intervalSet));
            }
        }
    }

    Y_UNIT_TEST(AddAndRemoveIntervalsRandomized)
    {
        constexpr int RoundsCount = 1000;
        constexpr ui64 IntervalSize = 30;
        constexpr ui64 MaxIntervalsInSet = 10;

        TOverlappingIntervalSet set;
        TVector<TInterval> intervalSet;
        TVector<int> expectedData(IntervalSize, 0);

        for (int round = 0; round < RoundsCount; round++)
        {
            // The probability to remove element increases with data.size()
            if (RandomNumber(MaxIntervalsInSet) < intervalSet.size()) {
                auto intervalIndex = RandomNumber(intervalSet.size());
                auto interval = intervalSet[intervalIndex];
                intervalSet.erase(intervalSet.begin() + intervalIndex);

                for (auto i = interval.Begin; i < interval.End; i++) {
                    expectedData[i]--;
                }

                set.RemoveInterval(interval.Begin, interval.End);

                ValidateOverlappingIntervalSet(set, intervalSet, expectedData);
            }

            auto interval = GenerateRandomInterval(IntervalSize);
            set.AddInterval(interval.Begin, interval.End);
            intervalSet.push_back(interval);

            for (auto i = interval.Begin; i < interval.End; i++) {
                expectedData[i]++;
            }

            ValidateOverlappingIntervalSet(set, intervalSet, expectedData);
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse
