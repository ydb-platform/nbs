#include "part_ut.h"

#include "part_events_private.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionCompressionTest)
{
    void DoShouldCompressCompactionBlobs(bool compressionEnabled)
    {
        constexpr ui32 dataChannelOffset = 3;
        constexpr ui32 blocksPerChunk = 4;
        constexpr ui32 blockCount = 2 * blocksPerChunk + 1;
        constexpr ui32 chunkSize = blocksPerChunk * DefaultBlockSize;
        const auto range = TBlockRange32::WithLength(0, blockCount);

        NProto::TStorageServiceConfig config;
        config.SetFlushBlobSizeThreshold(1_MB);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
        config.SetMergedBlobsCompressionEnabled(compressionEnabled);
        config.SetMergedBlobsCompressionChunckSize(chunkSize);
        config.SetVerifyBlobCompressionInfo(true);
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto partitionWithRuntime = SetupPartition(config);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.WriteBlocks(i, i + 1);
        }
        partition.Flush();
        partition.WriteBlocks(0, 100);
        partition.Flush();

        TVector<NKikimr::TLogoBlobID> writtenBlobs;
        TPartialBlobId compactedBlobId;
        std::optional<NProto::TBlobCompression> compressionInfo;
        ui32 compactionResults = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvBlobStorage::EvPut: {
                        const auto* msg =
                            event->Get<NKikimr::TEvBlobStorage::TEvPut>();
                        if (msg->Id.Channel() >= dataChannelOffset) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                msg->Id.BlobSize(),
                                msg->Buffer.size());
                            writtenBlobs.push_back(msg->Id);
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        const auto* msg = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == ADD_COMPACTION_RESULT) {
                            UNIT_ASSERT(msg->MixedBlobs.empty());
                            UNIT_ASSERT_VALUES_EQUAL(
                                1,
                                msg->MergedBlobs.size());
                            const auto& blob = msg->MergedBlobs.front();
                            compactedBlobId = blob.BlobId;
                            compressionInfo = blob.CompressionInfo;
                            ++compactionResults;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto statsBefore = partition.StatPartition()->Record.GetStats();
        partition.Compaction();
        const auto statsAfter = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount,
            statsAfter.GetSysWriteCounters().GetBlocksCount() -
                statsBefore.GetSysWriteCounters().GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount,
            statsAfter.GetRealSysWriteCounters().GetBlocksCount() -
                statsBefore.GetRealSysWriteCounters().GetBlocksCount());

        UNIT_ASSERT_VALUES_EQUAL(1, compactionResults);
        UNIT_ASSERT_VALUES_EQUAL(1, writtenBlobs.size());
        UNIT_ASSERT(
            writtenBlobs.front() == MakeBlobId(TestTabletId, compactedBlobId));
        UNIT_ASSERT_VALUES_EQUAL_C(
            compressionEnabled,
            compressionInfo.has_value(),
            "Compaction blob compression metadata must match "
            "MergedBlobsCompressionEnabled");
        if (compressionEnabled) {
            UNIT_ASSERT(
                compactedBlobId.BlobSize() < blockCount * DefaultBlockSize);
            UNIT_ASSERT(
                NProto::TBlobCompression::LZ4 == compressionInfo->GetCodec());
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                compressionInfo->GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(3, compressionInfo->ChunkSizesSize());
            ui64 compressedSize = 0;
            for (const auto size: compressionInfo->GetChunkSizes()) {
                UNIT_ASSERT(size > 0);
                compressedSize += size;
            }
            UNIT_ASSERT_VALUES_EQUAL(
                compactedBlobId.BlobSize(),
                compressedSize);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                blockCount * DefaultBlockSize,
                compactedBlobId.BlobSize());
        }

        for (const bool reboot: {false, true}) {
            if (reboot) {
                partition.RebootTablet();
                partition.WaitReady();
                // Read the persisted blob again through compaction.
                partition.WriteBlocks(0, 100);
                partition.Flush();
                writtenBlobs.clear();
                compactionResults = 0;
                partition.Compaction();
                UNIT_ASSERT_VALUES_EQUAL(1, compactionResults);
                UNIT_ASSERT_VALUES_EQUAL(1, writtenBlobs.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    compressionEnabled,
                    compressionInfo.has_value());
                partition.Cleanup();
            }
            const auto response = partition.ReadBlocks(range);
            for (ui32 i = 0; i < blockCount; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, i == 0 ? 100 : i + 1),
                    response->Record.GetBlocks().GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldCompressCompactionBlobs)
    {
        DoShouldCompressCompactionBlobs(false);
        DoShouldCompressCompactionBlobs(true);
    }

    Y_UNIT_TEST(ShouldReadSingleBlocksFromCompressedChunks)
    {
        constexpr ui32 blocksPerChunk = 4;
        constexpr ui32 blockCount = 2 * blocksPerChunk + 1;
        constexpr ui32 chunkSize = blocksPerChunk * DefaultBlockSize;

        NProto::TStorageServiceConfig config;
        config.SetFlushBlobSizeThreshold(1_MB);
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
        config.SetMergedBlobsCompressionEnabled(true);
        config.SetMergedBlobsCompressionChunckSize(chunkSize);
        config.SetVerifyBlobCompressionInfo(true);
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetHDDMaxBlobsPerRange(100);
        config.SetSSDMaxBlobsPerRange(100);
        auto partitionWithRuntime = SetupPartition(config);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.WriteBlocks(i, 'a' + i);
            if (i + 1 == blocksPerChunk) {
                partition.Flush();
            }
        }
        partition.Flush();

        TPartialBlobId compactedBlobId;
        NProto::TBlobCompression compressionInfo;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* msg = event->Get<
                        TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (msg->Mode == ADD_COMPACTION_RESULT) {
                        UNIT_ASSERT_VALUES_EQUAL(1, msg->MergedBlobs.size());
                        const auto& blob = msg->MergedBlobs.front();
                        UNIT_ASSERT(blob.CompressionInfo);
                        compactedBlobId = blob.BlobId;
                        compressionInfo = *blob.CompressionInfo;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        partition.Compaction();

        UNIT_ASSERT(compactedBlobId);
        UNIT_ASSERT_VALUES_EQUAL(chunkSize, compressionInfo.GetChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(3, compressionInfo.ChunkSizesSize());
        const auto blobId = MakeBlobId(TestTabletId, compactedBlobId);
        TVector<ui32> chunkOffsets;
        ui32 compressedSize = 0;
        for (const auto size: compressionInfo.GetChunkSizes()) {
            chunkOffsets.push_back(compressedSize);
            compressedSize += size;
        }
        UNIT_ASSERT_VALUES_EQUAL(blobId.BlobSize(), compressedSize);

        ui32 expectedChunk = 0;
        ui32 blobReads = 0;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    NKikimr::TEvBlobStorage::EvGet)
                {
                    const auto* msg =
                        event->Get<NKikimr::TEvBlobStorage::TEvGet>();
                    for (ui32 i = 0; i < msg->QuerySize; ++i) {
                        const auto& query = msg->Queries[i];
                        if (query.Id == blobId) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                chunkOffsets[expectedChunk], query.Shift);
                            UNIT_ASSERT_VALUES_EQUAL(
                                compressionInfo.GetChunkSizes(expectedChunk),
                                query.Size);
                            ++blobReads;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (const bool reboot: {false, true}) {
            if (reboot) {
                partition.RebootTablet();
                partition.WaitReady();
            }
            // Read every position within each chunk, including its boundaries
            // and the single block in the partial final chunk.
            for (ui32 i = 0; i < blockCount; ++i) {
                expectedChunk = i / blocksPerChunk;
                const TString expected(DefaultBlockSize, 'a' + i);
                blobReads = 0;
                const auto response = partition.ReadBlocks(i);
                UNIT_ASSERT_VALUES_EQUAL(
                    1, response->Record.GetBlocks().BuffersSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    expected, response->Record.GetBlocks().GetBuffers(0));
                UNIT_ASSERT_VALUES_EQUAL(1, blobReads);

                TString buffer(DefaultBlockSize, '?');
                blobReads = 0;
                partition.ReadBlocksLocal(i, TStringBuf(buffer));
                UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
                UNIT_ASSERT_VALUES_EQUAL(1, blobReads);
            }
        }
    }

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
