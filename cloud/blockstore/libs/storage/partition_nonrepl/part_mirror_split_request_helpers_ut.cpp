#include "part_mirror_split_request_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

using namespace NActors;

namespace {
TActorId MakeActorId(ui32 num)
{
    return TActorId(num, num, num, num);
}

TSgList UnifySglist(const TSgList& sglist)
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

        auto maybeSplittedRequest =
            NSplitRequest::SplitRequest<TEvService::TReadBlocksMethod>(
                request,
                blockRangeSplittedByDeviceBorders,
                TVector(actorsForEachRequests));

        UNIT_ASSERT(maybeSplittedRequest.has_value());
        auto splittedRequest = std::move(maybeSplittedRequest.value());
        UNIT_ASSERT_VALUES_EQUAL(
            splittedRequest.size(),
            actorsForEachRequests.size());

        for (size_t i = 0; i < splittedRequest.size(); ++i) {
            const auto& partSplitted = splittedRequest[i];
            const auto& partSplittedRequest = partSplitted.Request;
            UNIT_ASSERT_VALUES_EQUAL(
                partSplitted.BlockRangeForRequest,
                blockRangeSplittedByDeviceBorders[i]);

            UNIT_ASSERT_VALUES_EQUAL(
                partSplitted.Partitions.size(),
                actorsForEachRequests[i].size());
            for (const auto& actorId: partSplitted.Partitions) {
                UNIT_ASSERT(FindPtr(actorsForEachRequests[i], actorId));
            }

            UNIT_ASSERT_VALUES_EQUAL(
                request.GetDiskId(),
                partSplittedRequest.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetFlags(),
                partSplittedRequest.GetFlags());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetCheckpointId(),
                partSplittedRequest.GetCheckpointId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetSessionId(),
                partSplittedRequest.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                partSplittedRequest.GetStartIndex(),
                blockRangeSplittedByDeviceBorders[i].Start);
            UNIT_ASSERT_VALUES_EQUAL(
                partSplittedRequest.GetBlocksCount(),
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

        TVector<TVector<TActorId>> actorsForEachRequests{
            {MakeActorId(0), MakeActorId(1)},
            {MakeActorId(2), MakeActorId(3)},
            {MakeActorId(4), MakeActorId(5)},
            {MakeActorId(6), MakeActorId(7)},
        };

        auto maybeSplittedRequest =
            NSplitRequest::SplitRequest<TEvService::TReadBlocksLocalMethod>(
                request,
                blockRangeSplittedByDeviceBorders,
                TVector(actorsForEachRequests));

        UNIT_ASSERT(maybeSplittedRequest.has_value());
        auto splittedRequest = std::move(maybeSplittedRequest.value());
        UNIT_ASSERT_VALUES_EQUAL(
            splittedRequest.size(),
            actorsForEachRequests.size());

        TSgList overallSglist;
        for (size_t i = 0; i < splittedRequest.size(); ++i) {
            const auto& partSplitted = splittedRequest[i];
            const auto& partSplittedRequest = partSplitted.Request;
            UNIT_ASSERT_VALUES_EQUAL(
                partSplitted.BlockRangeForRequest,
                blockRangeSplittedByDeviceBorders[i]);

            UNIT_ASSERT_VALUES_EQUAL(
                partSplitted.Partitions.size(),
                actorsForEachRequests[i].size());
            for (const auto& actorId: partSplitted.Partitions) {
                UNIT_ASSERT(FindPtr(actorsForEachRequests[i], actorId));
            }

            UNIT_ASSERT_VALUES_EQUAL(
                request.GetDiskId(),
                partSplittedRequest.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetFlags(),
                partSplittedRequest.GetFlags());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetCheckpointId(),
                partSplittedRequest.GetCheckpointId());
            UNIT_ASSERT_VALUES_EQUAL(
                request.GetSessionId(),
                partSplittedRequest.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                blockRangeSplittedByDeviceBorders[i].Start,
                partSplittedRequest.GetStartIndex());
            UNIT_ASSERT_VALUES_EQUAL(
                blockRangeSplittedByDeviceBorders[i].Size(),
                partSplittedRequest.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                request.BlockSize,
                partSplittedRequest.BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(
                request.CommitId,
                partSplittedRequest.CommitId);

            auto guard = partSplittedRequest.Sglist.Acquire();
            const auto& splittedSglist = guard.Get();
            size_t overallSize = 0;
            for (size_t i = 0; i < splittedSglist.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    splittedSglist[i].Size() % request.BlockSize,
                    0);
                overallSize += splittedSglist[i].Size();
                overallSglist.emplace_back(splittedSglist[i]);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                overallSize,
                partSplittedRequest.BlockSize *
                    partSplittedRequest.GetBlocksCount());
        }

        auto unifiedSglist = UnifySglist(overallSglist);
        UNIT_ASSERT_VALUES_EQUAL(unifiedSglist.size(), sglist.size());
        for (size_t i = 0; i < unifiedSglist.size(); ++i) {
            UNIT_ASSERT_EQUAL(unifiedSglist[i].Data(), sglist[i].Data());
            UNIT_ASSERT_VALUES_EQUAL(unifiedSglist[i].Size(), sglist[i].Size());
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

        TVector<TVector<TActorId>> actorsForEachRequests{
            {MakeActorId(0), MakeActorId(1)},
        };

        auto maybeSplittedRequest =
            NSplitRequest::SplitRequest<TEvService::TReadBlocksLocalMethod>(
                request,
                blockRangeSplittedByDeviceBorders,
                actorsForEachRequests);

        UNIT_ASSERT(!maybeSplittedRequest.has_value());
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

        auto maybeSplittedRequest =
            NSplitRequest::SplitRequest<TEvService::TReadBlocksLocalMethod>(
                request,
                blockRangeSplittedByDeviceBorders,
                TVector(actorsForEachRequests));

        UNIT_ASSERT(!maybeSplittedRequest.has_value());
    }

    Y_UNIT_TEST(ShouldCorrectlyUnifyReadResponses)
    {
        TVector<NSplitRequest::TUnifyResponsesContext<
            TEvService::TReadBlocksMethod>>
            responses;

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
            responses.push_back({std::move(response), blocksCount});
        }

        auto unifiedResponse =
            UnifyResponses(MakeConstArrayRef(responses), blockSize);
        UNIT_ASSERT(!HasError(unifiedResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            unifiedResponse.GetThrottlerDelay(),
            throttlerDelaySum);
        UNIT_ASSERT(!unifiedResponse.GetAllZeroes());

        size_t blocksReviewed = 0;
        for (size_t blocksCount = 1; blocksCount <= iterationsCount;
             ++blocksCount)
        {
            for (size_t blockI = 0; blockI < blocksCount; ++blockI) {
                const auto& block =
                    unifiedResponse.GetBlocks().GetBuffers()[blocksReviewed];
                UNIT_ASSERT_VALUES_EQUAL(
                    block,
                    TString(blockSize, '0' + blocksCount));

                ++blocksReviewed;
            }
        }
    }

    Y_UNIT_TEST(ShouldFillZeroedResponses)
    {
        const size_t blockSize = 100;

        NProto::TReadBlocksResponse resp1;
        resp1.ClearBlocks();
        resp1.SetAllZeroes(true);

        NProto::TReadBlocksResponse resp2;
        resp2.ClearBlocks();
        resp2.SetAllZeroes(false);
        resp2.MutableBlocks()->AddBuffers(TString(blockSize, '1'));

        TVector<NSplitRequest::TUnifyResponsesContext<
            TEvService::TReadBlocksMethod>>
            responses{
                {.Response = resp1, .BlocksCountRequested = 1},
                {.Response = resp2, .BlocksCountRequested = 1},
            };

        auto unifiedResponse =
            UnifyResponses(MakeConstArrayRef(responses), blockSize);

        UNIT_ASSERT_VALUES_EQUAL(unifiedResponse.GetBlocks().BuffersSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            unifiedResponse.GetBlocks().GetBuffers()[0],
            TString(blockSize, '\0'));
        UNIT_ASSERT_VALUES_EQUAL(
            unifiedResponse.GetBlocks().GetBuffers()[1],
            TString(blockSize, '1'));
    }

    Y_UNIT_TEST(ShouldCorrectlyProcessAllZeros)
    {
        const size_t blockSize = 100;

        NProto::TReadBlocksResponse resp1;
        resp1.ClearBlocks();
        resp1.SetAllZeroes(true);

        NProto::TReadBlocksResponse resp2;
        resp2.ClearBlocks();
        resp2.SetAllZeroes(true);

        TVector<NSplitRequest::TUnifyResponsesContext<
            TEvService::TReadBlocksMethod>>
            responses{
                {.Response = resp1, .BlocksCountRequested = 1},
                {.Response = resp2, .BlocksCountRequested = 1},
            };

        auto unifiedResponse =
            UnifyResponses(MakeConstArrayRef(responses), blockSize);

        UNIT_ASSERT_VALUES_EQUAL(unifiedResponse.GetBlocks().BuffersSize(), 0);
        UNIT_ASSERT(unifiedResponse.GetAllZeroes());
    }

    Y_UNIT_TEST(ShouldCorrectlyProcessErrors)
    {
        const size_t blockSize = 100;

        NProto::TReadBlocksResponse resp1;
        resp1.MutableError()->CopyFrom(MakeError(E_REJECTED, "reject"));

        NProto::TReadBlocksResponse resp2;
        resp2.ClearBlocks();
        resp2.SetAllZeroes(true);

        TVector<NSplitRequest::TUnifyResponsesContext<
            TEvService::TReadBlocksMethod>>
            responses{
                {.Response = resp1, .BlocksCountRequested = 1},
                {.Response = resp2, .BlocksCountRequested = 1},
            };

        auto unifiedResponse =
            UnifyResponses(MakeConstArrayRef(responses), blockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            unifiedResponse.GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            unifiedResponse.GetError().GetMessage(),
            "reject");
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
