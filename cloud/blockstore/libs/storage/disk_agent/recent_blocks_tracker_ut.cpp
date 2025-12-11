#include "recent_blocks_tracker.h"

#include <cloud/blockstore/libs/storage/model/composite_id.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRecentBlocksTrackerTest)
{
    Y_UNIT_TEST(Basic)
    {
        constexpr size_t TRACK_DEPTH = 10;
        TCompositeId id = TCompositeId::FromGeneration(1);

        TRecentBlocksTracker tracker{"device1", TRACK_DEPTH};

        {
            // Insert first range.
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(
                    id.GetValue(),
                    TBlockRange64::WithLength(0, 1024),
                    &overlapDetails));
            tracker.AddRecorded(
                id.GetValue(),
                TBlockRange64::WithLength(0, 1024));
        }

        {
            // Same id is denied
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Unknown,
                tracker.CheckRecorded(
                    id.GetValue(),
                    TBlockRange64::WithLength(0, 1024),
                    &overlapDetails));
        }

        // The overlap status for a very old request is unknown anyway.
        auto veryOldId = id.Advance();
        for (size_t i = 0; i < TRACK_DEPTH; ++i) {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(
                    id.Advance(),
                    TBlockRange64::WithLength(0, 1024),
                    &overlapDetails));
            tracker.AddRecorded(
                id.GetValue(),
                TBlockRange64::WithLength(0, 1024));
        }

        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Unknown,
                tracker.CheckRecorded(
                    veryOldId,
                    TBlockRange64::WithLength(0, 1024),
                    &overlapDetails));
        }
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Unknown,
                tracker.CheckRecorded(
                    veryOldId,
                    TBlockRange64::WithLength(1024, 1024),
                    &overlapDetails));
        }

        // Any configuration is allowed for requests with increasing IDs
        for (int i = 0; i < 1000; ++i) {
            ui64 start = random() * 100;
            ui64 len = random() * 30;
            auto range = TBlockRange64::WithLength(start, len);
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(id.Advance(), range, &overlapDetails));
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

        for (const auto& check: rangeChecks) {
            TString overlapDetails;
            auto status = tracker.CheckRecorded(
                check.Id,
                TBlockRange64::MakeClosedInterval(check.Start, check.End),
                &overlapDetails);
            UNIT_ASSERT_VALUES_EQUAL_C(check.Status, status, check.Description);
            if (status == EOverlapStatus::NotOverlapped) {
                tracker.AddRecorded(
                    check.Id,
                    TBlockRange64::MakeClosedInterval(check.Start, check.End));
            }
        }
    }

    Y_UNIT_TEST(CheckingOnlyNewestIds)
    {
        TRecentBlocksTracker tracker("device1");

        tracker.AddRecorded(100, TBlockRange64::WithLength(0, 10));
        tracker.AddRecorded(102, TBlockRange64::MakeClosedInterval(10, 20));

        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(
                    101,
                    TBlockRange64::WithLength(0, 10),
                    &overlapDetails));
        }
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Complete,
                tracker.CheckRecorded(
                    99,
                    TBlockRange64::WithLength(0, 10),
                    &overlapDetails));
        }
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Partial,
                tracker.CheckRecorded(
                    101,
                    TBlockRange64::WithLength(0, 20),
                    &overlapDetails));
        }
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Complete,
                tracker.CheckRecorded(
                    99,
                    TBlockRange64::WithLength(0, 20),
                    &overlapDetails));
        }
    }

    Y_UNIT_TEST(InflightRequests)
    {
        TRecentBlocksTracker tracker("device1");

        tracker.AddInflight(100, TBlockRange64::WithLength(0, 10));
        tracker.AddInflight(102, TBlockRange64::MakeClosedInterval(20, 30));

        // Reject same id.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            tracker.CheckInflight(
                100,
                TBlockRange64::MakeClosedInterval(100, 101)));

        // Detect overlapping with greater id
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            tracker.CheckInflight(
                99,
                TBlockRange64::MakeClosedInterval(5, 15)));

        // Ignore overlapping with smaller id
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            tracker.CheckInflight(
                101,
                TBlockRange64::MakeClosedInterval(5, 15)));

        // Finish request with id=100.
        tracker.RemoveInflight(100);

        // Not overalpped with id=100 any more.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            tracker.CheckInflight(
                99,
                TBlockRange64::MakeClosedInterval(5, 15)));
    }

    Y_UNIT_TEST(AdvanceGeneration)
    {
        TCompositeId idGen1 = TCompositeId::FromGeneration(1);
        idGen1.Advance();
        TCompositeId idGen2 = TCompositeId::FromGeneration(2);

        UNIT_ASSERT_GT(idGen2.GetValue(), idGen1.GetValue());
    }

    Y_UNIT_TEST(AcceptAnyAfterReset)
    {
        TRecentBlocksTracker tracker("device1");

        tracker.AddRecorded(200, TBlockRange64::WithLength(0, 10));
        tracker.AddInflight(201, TBlockRange64::WithLength(10, 10));
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::Partial,
                tracker.CheckRecorded(
                    100,
                    TBlockRange64::WithLength(5, 10),
                    &overlapDetails));

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                tracker.CheckInflight(100, TBlockRange64::WithLength(5, 10)));
        }

        tracker.Reset();
        {
            TString overlapDetails;
            UNIT_ASSERT_VALUES_EQUAL(
                EOverlapStatus::NotOverlapped,
                tracker.CheckRecorded(
                    100,
                    TBlockRange64::WithLength(5, 10),
                    &overlapDetails));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                tracker.CheckInflight(100, TBlockRange64::WithLength(5, 10)));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
