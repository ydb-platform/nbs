#include "disjoint_interval_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

template<class TKey, class TValue>
TString Print(const TDisjointIntervalMap<TKey, TValue>& map)
{
    TStringBuilder sb;
    sb << "[";

    bool first = true;
    for (const auto& [_, value]: map) {
        if (first) {
            first = false;
        } else {
            sb << ", ";
        }
        sb << "(" << value.Begin << ", " << value.End << "): " << value.Value;
    }
    sb << "]";

    return sb;
}

template <class TKey, class TValue>
TString PrintOverlapping(
    TDisjointIntervalMap<TKey, TValue>& map,
    TKey begin,
    TKey end)
{
    TDisjointIntervalMap<TKey, TValue> tmp;

    map.VisitOverlapping(begin, end, [&](auto it) {
        tmp.Add(it->second.Begin, it->second.End, it->second.Value);
    });

    return Print(tmp);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using TMap = TDisjointIntervalMap<ui64, TString>;

Y_UNIT_TEST_SUITE(TDisjointIntervalMapTest)
{
    Y_UNIT_TEST(SimpleAdd)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");

        UNIT_ASSERT_VALUES_EQUAL("[(1, 3): vasya, (3, 5): petya]", Print(map));
    }

    Y_UNIT_TEST(VisitOverlapping)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");
        map.Add(9, 12, "ben");
        map.Add(6, 7, "igor");

        for (ui64 l = 0; l <= 14; l++) {
            for (ui64 r = l + 1; r <= 15; r++) {
                TMap expected;
                if (l < 3 && r > 1) {
                    expected.Add(1, 3, "vasya");
                }
                if (l < 5 && r > 3) {
                    expected.Add(3, 5, "petya");
                }
                if (l < 7 && r > 6) {
                    expected.Add(6, 7, "igor");
                }
                if (l < 12 && r > 9) {
                    expected.Add(9, 12, "ben");
                }

                UNIT_ASSERT_VALUES_EQUAL_C(
                    Print(expected),
                    PrintOverlapping(map, l, r),
                    "Failed for l=" << l << ", r=" << r);
            }
        }
    }

    Y_UNIT_TEST(DeleteWhileVisitOverlapping)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");
        map.Add(9, 12, "ben");
        map.Add(6, 7, "igor");

        map.VisitOverlapping(4UL, 13UL, [&](auto it) {
            if (it->second.Value == "igor") {
                map.Remove(it);
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(
            "[(1, 3): vasya, (3, 5): petya, (9, 12): ben]",
            Print(map));
    }
}

}   // namespace NCloud::NFileStore::NFuse
