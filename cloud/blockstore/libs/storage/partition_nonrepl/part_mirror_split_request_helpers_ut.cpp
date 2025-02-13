#include "part_mirror_split_request_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

namespace {
TActorId MakeActorId(ui32 num)
{
    return TActorId(num, num, num, num);
}

TSgList MergeSglist(const TSgList& sglist)
{
    TSgList result = {sglist[0]};
    for (size_t i = 1; i < sglist.size(); ++i) {
        if ((result.back().Data() + result.back().Size()) == sglist[i].Data()) {
            result.back() = TBlockDataRef(
                result.back().Data(),
                result.back().Size() + sglist[i].Size());
        } else {
            result.emplace_back(sglist[i]);
        }
    }

    return result;
}

}   // namespace

Y_UNIT_TEST_SUITE(TSplitRequestTest)
{
    Y_UNIT_TEST(ShouldSplitReadRequest)
    {
        NProto::TReadBlocksRequest request;

        const ui32 startBlock = 2;
        const ui32 blocksCount = 10;

        request.SetDiskId("disk-1");
        request.SetStartIndex(startBlock);
        request.SetBlocksCount(blocksCount);
        request.SetFlags(1234);   // just some random number
        request.SetCheckpointId("checkpoint-1");
        request.SetSessionId("session-1");

        TVector<TBlockRange64> blockRangeSplittedByDeviceBorders{
            // block range splitted by device borders, sizeof device == 4 blocks
            TBlockRange64::WithLength(2, 2),
            TBlockRange64::WithLength(4, 4),
            TBlockRange64::WithLength(8, 4),
            TBlockRange64::WithLength(12, 1),
        };

        TVector<TVector<TActorId>> actorsForEachRequests{
            {MakeActorId(0), MakeActorId(1)},
            {MakeActorId(2), MakeActorId(3)},
            {MakeActorId(4), MakeActorId(5)},
            {MakeActorId(6), MakeActorId(7)},
        };

        auto splitRequest =
            SplitReadRequest(request, blockRangeSplittedByDeviceBorders);

        UNIT_ASSERT(splitRequest);
        UNIT_ASSERT_VALUES_EQUAL(
            splitRequest.size(),
            actorsForEachRequests.size());

        for (size_t i = 0; i < splitRequest.size(); ++i) {
            const auto& partSplit = splitRequest[i];
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(
                    partSplit.GetStartIndex(),
                    partSplit.GetBlocksCount()),
                blockRangeSplittedByDeviceBorders[i]);

            UNIT_ASSERT_VALUES_EQUAL(
                request.GetDiskId(),
                partSplit.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(request.GetFlags(), partSplit.GetFlags());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetCheckpointId(),
                partSplit.GetCheckpointId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetSessionId(),
                partSplit.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                partSplit.GetStartIndex(),
                blockRangeSplittedByDeviceBorders[i].Start);
            UNIT_ASSERT_VALUES_EQUAL(
                partSplit.GetBlocksCount(),
                blockRangeSplittedByDeviceBorders[i].Size());
        }
    }

    Y_UNIT_TEST(ShouldSplitReadLocalRequest)
    {
        NProto::TReadBlocksLocalRequest request;

        const ui32 startBlock = 2;
        const ui32 blocksCount = 10;
        // const auto deviceSizeInBlocks = 4;
        const auto blockSize = 100;

        request.SetDiskId("disk-1");
        request.SetStartIndex(startBlock);
        request.SetBlocksCount(blocksCount);
        request.SetFlags(1234);   // just some random number
        request.SetCheckpointId("checkpoint-1");
        request.SetSessionId("session-1");
        request.BlockSize = blockSize;
        request.CommitId = 12345;

        // SplitRequest function doesn't access memory, so it's must be safe to
        // use random values
        TSgList sglist{
            TBlockDataRef(reinterpret_cast<const char*>(100), 1 * blockSize),
            TBlockDataRef(reinterpret_cast<const char*>(3000), 1 * blockSize),
            TBlockDataRef(reinterpret_cast<const char*>(5000), 5 * blockSize),
            TBlockDataRef(reinterpret_cast<const char*>(11000), 4 * blockSize),
        };

        request.Sglist = TGuardedSgList(sglist);

        TVector<TBlockRange64> blockRangeSplittedByDeviceBorders{
            // block range splitted by device borders, sizeof device == 4
            // blocks
            TBlockRange64::WithLength(2, 2),
            TBlockRange64::WithLength(4, 4),
            TBlockRange64::WithLength(8, 4),
            TBlockRange64::WithLength(12, 1),
        };

        auto splitRequest =
            SplitReadRequest(request, blockRangeSplittedByDeviceBorders);

        UNIT_ASSERT(splitRequest);
        UNIT_ASSERT_VALUES_EQUAL(
            splitRequest.size(),
            blockRangeSplittedByDeviceBorders.size());

        TSgList overallSglist;
        for (size_t i = 0; i < splitRequest.size(); ++i) {
            const auto& partSplit = splitRequest[i];
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(
                    partSplit.GetStartIndex(),
                    partSplit.GetBlocksCount()),
                blockRangeSplittedByDeviceBorders[i]);

            UNIT_ASSERT_VALUES_EQUAL(
                request.GetDiskId(),
                partSplit.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(request.GetFlags(), partSplit.GetFlags());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetCheckpointId(),
                partSplit.GetCheckpointId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetSessionId(),
                partSplit.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                blockRangeSplittedByDeviceBorders[i].Start,
                partSplit.GetStartIndex());
            UNIT_ASSERT_VALUES_EQUAL(
                blockRangeSplittedByDeviceBorders[i].Size(),
                partSplit.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(request.BlockSize, partSplit.BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(request.CommitId, partSplit.CommitId);

            auto guard = partSplit.Sglist.Acquire();
            const auto& splittedSglist = guard.Get();
            size_t overallSize = 0;
            for (auto buf: splittedSglist) {
                UNIT_ASSERT_VALUES_EQUAL(buf.Size() % request.BlockSize, 0);
                overallSize += buf.Size();
                overallSglist.emplace_back(buf);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                overallSize,
                partSplit.BlockSize * partSplit.GetBlocksCount());
        }

        auto mergedSglist = MergeSglist(overallSglist);
        UNIT_ASSERT_VALUES_EQUAL(mergedSglist.size(), sglist.size());
        for (size_t i = 0; i < mergedSglist.size(); ++i) {
            UNIT_ASSERT_EQUAL(mergedSglist[i].Data(), sglist[i].Data());
            UNIT_ASSERT_VALUES_EQUAL(mergedSglist[i].Size(), sglist[i].Size());
        }
    }

    Y_UNIT_TEST(ShouldHandleNotEnoughSglistBuffers)
    {
        NProto::TReadBlocksLocalRequest request;

        const ui32 startBlock = 0;
        const ui32 blocksCount = 3;
        const auto blockSize = 100;

        request.SetDiskId("disk-1");
        request.SetStartIndex(startBlock);
        request.SetBlocksCount(blocksCount);
        request.SetFlags(1234);   // just some random number
        request.SetCheckpointId("checkpoint-1");
        request.SetSessionId("session-1");
        request.BlockSize = blockSize;
        request.CommitId = 12345;

        // SplitRequest function doesn't access memory, so it's must be safe to
        // use random values
        TSgList sglist{
            TBlockDataRef(reinterpret_cast<const char*>(100), 2 * blockSize),
        };

        request.Sglist = TGuardedSgList(sglist);

        TVector<TBlockRange64> blockRangeSplittedByDeviceBorders{
            // block range splitted by device borders, sizeof device == 4
            // blocks
            TBlockRange64::WithLength(0, 3),
        };

        auto splitRequest =
            SplitReadRequest(request, blockRangeSplittedByDeviceBorders);

        UNIT_ASSERT(!splitRequest);
    }

    Y_UNIT_TEST(ShouldHandleClosedSglist)
    {
        NProto::TReadBlocksLocalRequest request;

        const ui32 startBlock = 0;
        const ui32 blocksCount = 3;
        const auto blockSize = 100;

        request.SetDiskId("disk-1");
        request.SetStartIndex(startBlock);
        request.SetBlocksCount(blocksCount);
        request.SetFlags(1234);   // just some random number
        request.SetCheckpointId("checkpoint-1");
        request.SetSessionId("session-1");
        request.BlockSize = blockSize;
        request.CommitId = 12345;

        // SplitRequest function doesn't access memory, so it's must be safe to
        // use random values
        TSgList sglist{
            TBlockDataRef(reinterpret_cast<const char*>(100), 3 * blockSize),
        };

        TGuardedSgList guardedSglist(sglist);

        request.Sglist = guardedSglist;

        TVector<TBlockRange64> blockRangeSplittedByDeviceBorders{
            // block range splitted by device borders, sizeof device == 4
            // blocks
            TBlockRange64::WithLength(0, 3),
        };

        TVector<TVector<TActorId>> actorsForEachRequests{
            {MakeActorId(0), MakeActorId(1)},
        };

        guardedSglist.Close();

        auto splitRequest =
            SplitReadRequest(request, blockRangeSplittedByDeviceBorders);

        UNIT_ASSERT(!splitRequest);
    }

    Y_UNIT_TEST(ShouldCorrectlyMergeReadResponses)
    {
        TVector<NProto::TReadBlocksResponse> responses;

        const size_t iterationsCount = 20;

        const size_t blockSize = 100;
        size_t throttlerDelaySum = 0;
        for (size_t blocksCount = 1; blocksCount <= iterationsCount;
             ++blocksCount)
        {
            NProto::TReadBlocksResponse response;

            for (size_t blockI = 0; blockI < blocksCount; ++blockI) {
                response.MutableBlocks()->AddBuffers(
                    TString(blockSize, '0' + blocksCount));
            }

            TDynBitMap map;
            map.Reserve(blocksCount);
            map.Clear();
            if (blocksCount % 2) {
                map.Flip();
            }

            response.SetThrottlerDelay(blocksCount);
            throttlerDelaySum += blocksCount;
            response.SetAllZeroes(false);
            responses.emplace_back(std::move(response));
        }

        auto mergedResponse = MergeReadResponses(responses);
        UNIT_ASSERT(!HasError(mergedResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            mergedResponse.GetThrottlerDelay(),
            throttlerDelaySum);
        UNIT_ASSERT(!mergedResponse.GetAllZeroes());

        size_t blocksReviewed = 0;
        for (size_t blocksCount = 1; blocksCount <= iterationsCount;
             ++blocksCount)
        {
            for (size_t blockI = 0; blockI < blocksCount; ++blockI) {
                const auto& block =
                    mergedResponse.GetBlocks().GetBuffers(blocksReviewed);
                UNIT_ASSERT_VALUES_EQUAL(
                    block,
                    TString(blockSize, '0' + blocksCount));

                ++blocksReviewed;
            }
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyProcessErrors)
    {
        NProto::TReadBlocksResponse resp1;
        resp1.MutableError()->CopyFrom(MakeError(E_REJECTED, "reject"));

        NProto::TReadBlocksResponse resp2;
        resp2.ClearBlocks();
        resp2.SetAllZeroes(true);

        TVector<NProto::TReadBlocksResponse> responses{resp1, resp2};

        auto mergedResponse = MergeReadResponses(responses);

        UNIT_ASSERT_VALUES_EQUAL(
            mergedResponse.GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            mergedResponse.GetError().GetMessage(),
            "reject");
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
