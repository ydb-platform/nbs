#include "actor_loadfreshblobs.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckNonOverlappingRanges(
    const TVector<TGroupRange>& requests,
    TVector<NKikimr::TTabletChannelInfo::THistoryEntry> history,
    ui64 barrierCommitId)
{
        UNIT_ASSERT_GE(history.size(), requests.size());

        if (requests.empty()) {
            return;
        }

        // check that first group request is above barrierCommitId
        {
            UNIT_ASSERT_VALUES_EQUAL(barrierCommitId, requests[0].FromCommit);
            if (requests.size() == 1) {
                UNIT_ASSERT_VALUES_EQUAL(
                    Max<ui32>(),
                    ParseCommitId(requests[0].ToCommit).first);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    history[1].FromGeneration - 1,
                    ParseCommitId(requests[0].ToCommit).first);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                Max<ui32>(),
                ParseCommitId(requests[0].ToCommit).second);
            UNIT_ASSERT_VALUES_EQUAL(history[0].GroupID, requests[0].GroupId);
        }

        // check rest of the ranges
        for (ui32 i = 1; i < requests.size() - 1; ++i) {
            auto historyStartCommitId = MakeCommitId(
                history[i].FromGeneration,
                0);
            auto historyEndCommitId = MakeCommitId(
                history[i + 1].FromGeneration - 1,
                Max<ui32>());

            UNIT_ASSERT_VALUES_EQUAL(
                historyStartCommitId,
                requests[i].FromCommit);
            UNIT_ASSERT_VALUES_EQUAL(historyEndCommitId, requests[i].ToCommit);
            UNIT_ASSERT_VALUES_EQUAL(history[i].GroupID, requests[i].GroupId);
        }

        if (requests.size() > 1) {
            // check that last group request reads up to max commit
            auto historyStartCommitId = MakeCommitId(
                history.back().FromGeneration,
                0);
            auto historyEndCommitId = MakeCommitId(Max<ui32>(), Max<ui32>());

            UNIT_ASSERT_VALUES_EQUAL(
                historyStartCommitId,
                requests.back().FromCommit);
            UNIT_ASSERT_VALUES_EQUAL(
                historyEndCommitId,
                requests.back().ToCommit);
            UNIT_ASSERT_VALUES_EQUAL(
                history.back().GroupID,
                requests.back().GroupId);
        }
}

};  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLoadFreshBlobsTests)
{
    Y_UNIT_TEST(ShouldGenerateNonOverlappingRangesWhenNoBarrier)
    {
        TVector<NKikimr::TTabletChannelInfo::THistoryEntry> history {
            {0, 0},
            {2, 1},
            {4, 2},
            {10, 1},
            {15, 4}
        };

        auto barrierCommitId = MakeCommitId(0, 0);

        CheckNonOverlappingRanges(
            BuildGroupRequestsForChannel(
                history,
                0,
                barrierCommitId),
            history,
            barrierCommitId);
    }


    Y_UNIT_TEST(ShouldGenerateNonOverlappingRangesWhenBarrierIsSet)
    {
        TVector<NKikimr::TTabletChannelInfo::THistoryEntry> history {
            {0, 0},
            {2, 1},
            {4, 2},
            {10, 1},
            {15, 4}
        };

        TVector<NKikimr::TTabletChannelInfo::THistoryEntry> expected {
            {4, 2},
            {10, 1},
            {15, 4}
        };

        auto barrierCommitId = MakeCommitId(5, 4);

        CheckNonOverlappingRanges(
            BuildGroupRequestsForChannel(
                history,
                0,
                barrierCommitId),
            expected,
            barrierCommitId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
