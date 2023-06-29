#include "recent_blocks_tracker.h"

#include <cloud/blockstore/libs/storage/model/composite_id.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator<<(IOutputStream& out, EOverlapStatus rhs)
{
    switch (rhs) {
        case EOverlapStatus::NotOverlapped:
            out << "EOverlapStatus::NotOverlapped";
            break;
        case EOverlapStatus::Complete:
            out << "EOverlapStatus::Complete";
            break;
        case EOverlapStatus::Partial:
            out << "EOverlapStatus::Partial";
            break;
        case EOverlapStatus::Unknown:
            out << "EOverlapStatus::Unknown";
            break;
    }
    return out;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRecentBlocksTrackerTest)
{
    Y_UNIT_TEST(Basic)
    {
        constexpr size_t TRACK_DEPTH = 10;
        TCompositeId id = TCompositeId::FromGeneration(1);

        TRecentBlocksTracker tracker{"device1", TRACK_DEPTH};

        // Insert first range.
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::NotOverlapped,
            tracker.CheckRecorded(id.GetValue(), TBlockRange64{0, 1023}));
        tracker.AddRecorded(id.GetValue(), TBlockRange64{0, 1023});

        // Same id is denied
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Unknown,
            tracker.CheckRecorded(id.GetValue(), TBlockRange64{0, 1023}));

        // The overlap status for a very old request is unknown anyway.
        auto veryOldId = id.Advance();
        for (size_t i = 0; i < TRACK_DEPTH; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(id.Advance(), TBlockRange64{0, 1023}));
            tracker.AddRecorded(id.GetValue(), TBlockRange64{0, 1023});
        }
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Unknown,
            tracker.CheckRecorded(veryOldId, TBlockRange64{0, 1023}));
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Unknown,
            tracker.CheckRecorded(veryOldId, TBlockRange64{1024, 2047}));

        // Any configuration is allowed for requests with increasing IDs
        for (int i = 0; i < 1000; ++i) {
            ui64 start = random() * 100;
            ui64 len = random() * 30;
            TBlockRange64 range{start, start + len};
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(id.Advance(), range));
            tracker.AddRecorded(id.GetValue(), range);
        }
    }

    Y_UNIT_TEST(Overlaping)
    {
        TRecentBlocksTracker tracker("device1");

        struct RangeCheck
        {
            ui64 Id;
            ui64 Start;
            ui64 End;
            EOverlapStatus Status;
            const char* Description = nullptr;
        };
        constexpr RangeCheck rangeChecks[] = {
            {100, 10, 15, EOverlapStatus::NotOverlapped, "10-15"},
            {99, 0, 12, EOverlapStatus::Partial, "left overlap 10-12"},
            {98, 12, 20, EOverlapStatus::Partial, "right overlap 12-15"},
            {97, 0, 20, EOverlapStatus::Partial, "center overlap 10-15"},
            {96, 0, 9, EOverlapStatus::NotOverlapped, "0-9"},
            {95, 0, 12, EOverlapStatus::Complete, "full overlap {0-9}+{10-15}"},
            {94, 17, 20, EOverlapStatus::NotOverlapped, "17-20"},
            {93,
             0,
             20,
             EOverlapStatus::Partial,
             "partial {0-9}+{10-15}+{17-20}"},
            {92, 16, 16, EOverlapStatus::NotOverlapped, "16"},
            {91,
             0,
             20,
             EOverlapStatus::Complete,
             "full {0-9}+{10-15}+{16}+{17-20}"},
        };

        for (const auto& check : rangeChecks) {
            auto status = tracker.CheckRecorded(
                check.Id,
                TBlockRange64{check.Start, check.End});
            UNIT_ASSERT_VALUES_EQUAL_C(check.Status, status, check.Description);
            if (status == EOverlapStatus::NotOverlapped) {
                tracker.AddRecorded(
                    check.Id,
                    TBlockRange64{check.Start, check.End});
            }
        }
    }

    Y_UNIT_TEST(CheckingOnlyNewestIds)
    {
        TRecentBlocksTracker tracker("device1");

        tracker.AddRecorded(100, TBlockRange64{0, 9});
        tracker.AddRecorded(102, TBlockRange64{10, 20});

        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::NotOverlapped,
            tracker.CheckRecorded(101, TBlockRange64{0, 9}));
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Complete,
            tracker.CheckRecorded(99, TBlockRange64{0, 9}));

        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Partial,
            tracker.CheckRecorded(101, TBlockRange64{0, 20}));
        UNIT_ASSERT_VALUES_EQUAL(
            EOverlapStatus::Complete,
            tracker.CheckRecorded(99, TBlockRange64{0, 20}));
    }

    Y_UNIT_TEST(InflightRequests)
    {
        TRecentBlocksTracker tracker("device1");

        tracker.AddInflight(100, TBlockRange64{0, 9});
        tracker.AddInflight(102, TBlockRange64{20, 30});

        // Reject same id.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            tracker.CheckInflight(100, TBlockRange64{100, 101}));

        // Detect overlapping with greater id
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            tracker.CheckInflight(99, TBlockRange64{5, 15}));

        // Ignore overlapping with smaller id
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            tracker.CheckInflight(101, TBlockRange64{5, 15}));

        // Finish request with id=100.
        tracker.RemoveInflight(100);

        // Not overalpped with id=100 any more.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            tracker.CheckInflight(99, TBlockRange64{5, 15}));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
