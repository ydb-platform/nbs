#include "part_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompressionTest)
{
    Y_UNIT_TEST(ShouldRejectReadBlocksWithIncompleteBlobCompressionInfo)
    {
        NProto::TStorageServiceConfig config;
        config.SetVerifyBlobCompressionInfo(true);

        const TPartitionContent baseContent = {TBlob(1, 1), TBlob(2, 2)};
        auto partitionWithRuntime = SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            baseContent,
            {},
            DefaultBlockSize,
            1024,
            config,
            {});
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        ui32 compressionInfoCount = 0;
        ui32 describeResponses = 0;
        ui32 blobReads = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvVolume::EvDescribeBlocksResponse: {
                    auto& record = event->Get<
                        TEvVolume::TEvDescribeBlocksResponse>()->Record;
                    // Let the describe actor validate the complete response.
                    for (const auto& piece: record.GetBlobPieces()) {
                        auto* info = record.AddBlobId2CompressionInfo();
                        *info->MutableBlobId() = piece.GetBlobId();
                    }
                    break;
                }
                case TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::EventType: {
                    auto* msg = event->Get<
                        TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted>();
                    if (!msg->BlockMarks.empty()) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            2, msg->BlobId2CompressionInfo.size());
                        // Remove metadata before the partition read actor sees it.
                        msg->BlobId2CompressionInfo.resize(compressionInfoCount);
                        ++describeResponses;
                    }
                    break;
                }
                case TEvPartitionCommonPrivate::EvReadBlobRequest: {
                    ++blobReads;
                    break;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        const auto range = TBlockRange32::WithLength(0, baseContent.size());
        for (const ui32 count: {0, 1, 2}) {
            compressionInfoCount = count;
            const bool complete = count == baseContent.size();
            const auto checkResponse = [&](const auto& response) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    complete ? S_OK : E_REJECTED,
                    response->GetStatus(),
                    response->GetErrorReason());
                if (!complete) {
                    UNIT_ASSERT_STRING_CONTAINS(
                        response->GetErrorReason(),
                        "Compression info not found for blob");
                    UNIT_ASSERT_VALUES_EQUAL(0, blobReads);
                }
            };

            partition.SendReadBlocksRequest(range);
            auto response = partition.RecvReadBlocksResponse();
            checkResponse(response);
            if (complete) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, char(1)),
                    response->Record.GetBlocks().GetBuffers(0));
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, char(2)),
                    response->Record.GetBlocks().GetBuffers(1));
            }

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks, range.Size(), TString(DefaultBlockSize, '?'));
            partition.SendReadBlocksLocalRequest(range, std::move(sglist));
            auto localResponse = partition.RecvReadBlocksLocalResponse();
            checkResponse(localResponse);
            if (complete) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, char(1)), blocks[0]);
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, char(2)), blocks[1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(2 * (count + 1), describeResponses);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
