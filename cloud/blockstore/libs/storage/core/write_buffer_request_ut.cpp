#include "write_buffer_request.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/shuffle.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TRequestInBuffer<TWriteBufferRequestData> Req(ui32 start, ui32 end)
{
    return {
        end - start + 1,
        {
            nullptr,
            TBlockRange32::MakeClosedInterval(start, end),
            nullptr,
            false
        }
    };
}

TString ToString(
    const TVector<TRequestInBuffer<TWriteBufferRequestData>>& requests,
    const TRequestGrouping& g)
{
    TStringBuilder sb;
    for (const auto& group: g.Groups) {
        if (sb.size()) {
            sb << "|";
        }
        sb << "GROUP:W=" << group.Weight;
        for (const auto* request: group.Requests) {
            sb << ";w=" << request->Weight
                << ",r=" << request->Data.Range.Start
                << "-" << request->Data.Range.End;
        }
    }

    for (auto* request = g.FirstUngrouped; request != requests.end(); ++request) {
        if (request == g.FirstUngrouped) {
            if (g.Groups.size()) {
                sb << "|";
            }
            sb << "UNGROUPED";
        }
        sb << ";w=" << request->Weight
            << ",r=" << request->Data.Range.Start
            << "-" << request->Data.Range.End;
    }

    return sb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBufferRequestTest)
{
    Y_UNIT_TEST(TestGrouping)
    {
        TVector<TRequestInBuffer<TWriteBufferRequestData>> requests {
            Req(0, 5),      // kept intact
            Req(2, 3),      // deleted      -2
            Req(7, 11),     // cut          -2
            Req(10, 15),    // kept intact
            Req(16, 20),    // kept intact
            Req(21, 30),    // cut          -6
            Req(21, 25),    // deleted      -5
            Req(22, 27),    // deleted      -6
            Req(25, 32),    // cut          -6
            Req(27, 35),    // kept intact
        };

        ui32 totalWeight = 0;
        for (const auto& r: requests) {
            totalWeight += r.Weight;
        }

        // all in one group
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                0,              // min weight
                Max<ui32>(),    // max weight
                Max<ui32>()     // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "GROUP:W=35"
                ";w=6,r=0-5"
                ";w=0,r=2-3"
                ";w=3,r=7-9"
                ";w=6,r=10-15"
                ";w=5,r=16-20"
                ";w=4,r=21-24"
                ";w=0,r=21-25"
                ";w=0,r=22-27"
                ";w=2,r=25-26"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }

        // all ungrouped
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                Max<ui32>(),    // min weight
                Max<ui32>(),    // max weight
                Max<ui32>()     // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "UNGROUPED"
                ";w=6,r=0-5"
                ";w=0,r=2-3"
                ";w=3,r=7-9"
                ";w=6,r=10-15"
                ";w=5,r=16-20"
                ";w=4,r=21-24"
                ";w=0,r=21-25"
                ";w=0,r=22-27"
                ";w=2,r=25-26"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }

        // split by max range
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                10,             // min weight
                Max<ui32>(),    // max weight
                11              // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "GROUP:W=11"
                ";w=6,r=0-5"
                ";w=0,r=2-3"
                ";w=5,r=7-11"
                "|GROUP:W=11"
                ";w=6,r=10-15"
                ";w=5,r=16-20"
                "|GROUP:W=12"
                ";w=4,r=21-24"
                ";w=0,r=21-25"
                ";w=0,r=22-27"
                ";w=8,r=25-32"
                "|UNGROUPED"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }

        // split by max weight
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                10,             // min weight
                11,             // max weight
                Max<ui32>()     // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "GROUP:W=11"
                ";w=6,r=0-5"
                ";w=0,r=2-3"
                ";w=5,r=7-11"
                "|GROUP:W=11"
                ";w=6,r=10-15"
                ";w=5,r=16-20"
                "|GROUP:W=10"
                ";w=10,r=21-30"
                ";w=0,r=21-25"
                ";w=0,r=22-27"
                "|GROUP:W=11"
                ";w=2,r=25-26"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }

        // testing tiny max weight
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                0,              // min weight
                2,              // max weight
                Max<ui32>()     // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "GROUP:W=6"
                ";w=6,r=0-5"
                ";w=0,r=2-3"
                "|GROUP:W=5"
                ";w=5,r=7-11"
                "|GROUP:W=6"
                ";w=6,r=10-15"
                "|GROUP:W=5"
                ";w=5,r=16-20"
                "|GROUP:W=10"
                ";w=10,r=21-30"
                ";w=0,r=21-25"
                ";w=0,r=22-27"
                "|GROUP:W=8"
                ";w=8,r=25-32"
                "|GROUP:W=9"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }
    }

    Y_UNIT_TEST(TestGroupingFastPath)
    {
        TVector<TRequestInBuffer<TWriteBufferRequestData>> requests {
            Req(0, 5),
            Req(7, 9),
            Req(10, 15),
            Req(16, 20),
            Req(21, 24),
            Req(25, 26),
            Req(27, 35),
        };

        ui32 totalWeight = 0;
        for (const auto& r: requests) {
            totalWeight += r.Weight;
        }

        // all ungrouped
        {
            auto requestsCopy = requests;
            Shuffle(requestsCopy.begin(), requestsCopy.end());
            auto g = GroupRequests(
                requestsCopy,
                totalWeight,
                Max<ui32>(),    // min weight
                Max<ui32>(),    // max weight
                Max<ui32>()     // max range
            );

            UNIT_ASSERT_VALUES_EQUAL(
                "UNGROUPED"
                ";w=6,r=0-5"
                ";w=3,r=7-9"
                ";w=6,r=10-15"
                ";w=5,r=16-20"
                ";w=4,r=21-24"
                ";w=2,r=25-26"
                ";w=9,r=27-35",
                ToString(requestsCopy, g)
            );
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
