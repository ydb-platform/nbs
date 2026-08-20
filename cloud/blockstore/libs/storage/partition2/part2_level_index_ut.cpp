#include "part2.h"

#include "part2_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/testlib/part2_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVVLvlIdx");

NProto::TStorageServiceConfig DefaultConfig()
{
    NProto::TStorageServiceConfig config;
    config.SetFlushBlobSizeThreshold(4_KB);
    config.SetFreshByteCountThresholdForBackpressure(400_KB);
    config.SetFreshByteCountLimitForBackpressure(1200_KB);
    config.SetFreshByteCountFeatureMaxValue(6);
    config.SetCollectGarbageThreshold(10);
    config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    config.SetPassTraceIdToBlobstorage(true);
    return std::make_shared<TDiagnosticsConfig>(std::move(config));
}

class TDummyActor final: public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    const NProto::TStorageServiceConfig& config,
    ui32 blockCount)
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    runtime->AddLocalService(
        VolumeActorId,
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));
    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    SetupTabletServices(*runtime);

    auto tabletInfo = std::unique_ptr<TTabletStorageInfo>(
        CreateTestTabletInfo(TestTabletId, TTabletTypes::BlockStorePartition2));

    const ui32 channelCount = tabletInfo->Channels.size();
    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig()));

    NProto::TPartitionConfig partitionConfig;
    partitionConfig.SetDiskId("test");
    partitionConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_DEFAULT);
    partitionConfig.SetBlockSize(DefaultBlockSize);
    partitionConfig.SetBlocksCount(blockCount);

    auto* channelProfiles = partitionConfig.MutableExplicitChannelProfiles();
    channelProfiles->Add()->SetDataKind(
        static_cast<ui32>(EChannelDataKind::System));
    channelProfiles->Add()->SetDataKind(
        static_cast<ui32>(EChannelDataKind::Log));
    channelProfiles->Add()->SetDataKind(
        static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount - DataChannelOffset - 1; ++i) {
        channelProfiles->Add()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Merged));
    }

    channelProfiles->Add()->SetDataKind(
        static_cast<ui32>(EChannelDataKind::Fresh));

    auto diagnosticsConfig = CreateTestDiagnosticsConfig();
    auto createFunc = [=](const TActorId& owner, TTabletStorageInfo* info)
    {
        return CreatePartitionTablet(
                   owner,
                   info,
                   storageConfig,
                   diagnosticsConfig,
                   CreateProfileLogStub(),
                   CreateBlockDigestGeneratorStub(),
                   partitionConfig,
                   EStorageAccessMode::Default,
                   0,   // partitionIndex
                   1,   // siblingCount
                   VolumeActorId,
                   0)   // volumeTabletId
            .release();
    };

    auto bootstrapper =
        CreateTestBootstrapper(*runtime, tabletInfo.release(), createFunc);
    runtime->EnableScheduleForActor(bootstrapper);

    return runtime;
}

void AssertDescribeBlockContent(
    TPartitionClient& partition,
    ui32 blockIndex,
    const TString& checkpointId,
    const TLogoBlobID& expectedBlobId,
    char expectedContent)
{
    const auto response = partition.DescribeBlocks(
        TBlockRange32::MakeOneBlock(blockIndex),
        checkpointId);

    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(0, response->Record.FreshBlockRangesSize());
    UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());

    const auto& blobPiece = response->Record.GetBlobPieces(0);
    UNIT_ASSERT_VALUES_EQUAL(1, blobPiece.RangesSize());
    const auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
    UNIT_ASSERT_VALUES_EQUAL(expectedBlobId, blobId);

    const auto& range = blobPiece.GetRanges(0);
    UNIT_ASSERT_VALUES_EQUAL(blockIndex, range.GetBlockIndex());
    UNIT_ASSERT_VALUES_EQUAL(1, range.GetBlocksCount());

    TVector<TString> blocks;
    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char{0}));

    const auto readResponse = partition.ReadBlob(
        blobId,
        blobPiece.GetBSGroupId(),
        TVector<ui16>{static_cast<ui16>(range.GetBlobOffset())},
        sglist);

    UNIT_ASSERT_VALUES_EQUAL(S_OK, readResponse->GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(
        TString(DefaultBlockSize, expectedContent),
        sglist.front().AsStringBuf());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition2LevelIndexTest)
{
    Y_UNIT_TEST(ShouldFlushFreshBlocksToL0Index)
    {
        constexpr ui32 L0RangeBlockCount = 4;
        constexpr ui32 BlockCount = 3 * L0RangeBlockCount;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(2 * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '0');
        partition.WriteBlocks(3, 'a');
        partition.CreateCheckpoint("checkpoint-1");
        partition.WriteBlocks(3, 'b');
        partition.CreateCheckpoint("checkpoint-2");
        partition.WriteBlocks(3, 'c');
        partition.WriteBlocks(4, '4');
        partition.WriteBlocks(7, '7');
        partition.WriteBlocks(8, '8');
        partition.WriteBlocks(11, 'B');

        ui32 flushAddBlobsRequestCount = 0;
        TVector<TVector<ui32>> l0BlobBlockIndices;
        TVector<TVector<ui64>> l0BlobCommitIds;
        TMap<ui32, TLogoBlobID> l0BlobIdsByRange;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* request =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (request->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                        ++flushAddBlobsRequestCount;

                        UNIT_ASSERT(request->MixedBlobs.empty());
                        UNIT_ASSERT(request->MergedBlobs.empty());
                        UNIT_ASSERT(request->FreshBlobs.empty());
                        UNIT_ASSERT(request->L1Blobs.empty());

                        for (const auto& blob: request->L0Blobs) {
                            l0BlobBlockIndices.push_back(blob.BlockIndices);
                            l0BlobCommitIds.push_back(blob.CommitIds);

                            UNIT_ASSERT(!blob.BlockIndices.empty());
                            const ui32 rangeIndex =
                                blob.BlockIndices.front() / L0RangeBlockCount;
                            const bool inserted =
                                l0BlobIdsByRange
                                    .emplace(
                                        rangeIndex,
                                        MakeBlobId(TestTabletId, blob.BlobId))
                                    .second;
                            UNIT_ASSERT(inserted);
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(1, flushAddBlobsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(3, l0BlobBlockIndices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            l0BlobBlockIndices.size(),
            l0BlobCommitIds.size());
        UNIT_ASSERT_VALUES_EQUAL(3, l0BlobIdsByRange.size());

        TVector<ui32> flushedBlockIndices;
        for (size_t i = 0; i < l0BlobBlockIndices.size(); ++i) {
            const auto& blockIndices = l0BlobBlockIndices[i];
            const auto& commitIds = l0BlobCommitIds[i];

            UNIT_ASSERT(!blockIndices.empty());
            UNIT_ASSERT_VALUES_EQUAL(blockIndices.size(), commitIds.size());
            UNIT_ASSERT_VALUES_EQUAL(
                blockIndices.front() / L0RangeBlockCount,
                blockIndices.back() / L0RangeBlockCount);

            for (ui64 commitId: commitIds) {
                UNIT_ASSERT(commitId != 0);
            }

            flushedBlockIndices.insert(
                flushedBlockIndices.end(),
                blockIndices.begin(),
                blockIndices.end());
        }

        Sort(flushedBlockIndices);
        const TVector<ui32> expectedFlushedBlockIndices = {
            0,
            3,
            3,
            3,
            4,
            7,
            8,
            11,
        };
        UNIT_ASSERT_VALUES_EQUAL(
            expectedFlushedBlockIndices,
            flushedBlockIndices);

        const auto describeResponse =
            partition.DescribeBlocks(TBlockRange32::WithLength(0, BlockCount));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, describeResponse->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            describeResponse->Record.FreshBlockRangesSize());
        UNIT_ASSERT_VALUES_EQUAL(3, describeResponse->Record.BlobPiecesSize());

        TVector<ui32> describedBlockIndices;
        for (const auto& blobPiece: describeResponse->Record.GetBlobPieces()) {
            UNIT_ASSERT(blobPiece.RangesSize() != 0);

            const ui32 rangeIndex =
                blobPiece.GetRanges(0).GetBlockIndex() / L0RangeBlockCount;
            const auto expectedBlobId = l0BlobIdsByRange.find(rangeIndex);
            UNIT_ASSERT(expectedBlobId != l0BlobIdsByRange.end());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedBlobId->second,
                LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId()));

            for (const auto& range: blobPiece.GetRanges()) {
                for (ui32 i = 0; i < range.GetBlocksCount(); ++i) {
                    const ui32 blockIndex = range.GetBlockIndex() + i;
                    UNIT_ASSERT_VALUES_EQUAL(
                        rangeIndex,
                        blockIndex / L0RangeBlockCount);
                    describedBlockIndices.push_back(blockIndex);
                }
            }
        }

        Sort(describedBlockIndices);
        const TVector<ui32> expectedDescribedBlockIndices = {
            0,
            3,
            4,
            7,
            8,
            11,
        };
        UNIT_ASSERT_VALUES_EQUAL(
            expectedDescribedBlockIndices,
            describedBlockIndices);

        const auto versionedBlockBlobId = l0BlobIdsByRange.find(0);
        UNIT_ASSERT(versionedBlockBlobId != l0BlobIdsByRange.end());

        AssertDescribeBlockContent(
            partition,
            3,
            "checkpoint-1",
            versionedBlockBlobId->second,
            'a');
        AssertDescribeBlockContent(
            partition,
            3,
            "checkpoint-2",
            versionedBlockBlobId->second,
            'b');
        AssertDescribeBlockContent(
            partition,
            3,
            {},
            versionedBlockBlobId->second,
            'c');
    }

    Y_UNIT_TEST(ShouldPromoteSpecifiedL0RangeToL1Index)
    {
        constexpr ui32 L0RangeBlockCount = 8;
        constexpr ui32 L1RangeBlockCount = 4;
        constexpr ui32 PromotedRangeIndex = 1;
        constexpr ui32 BlockCount = 2 * L0RangeBlockCount;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '0');
        partition.WriteBlocks(7, '7');
        partition.WriteBlocks(8, 'a');
        partition.CreateCheckpoint("checkpoint-1");
        partition.WriteBlocks(8, 'b');
        partition.CreateCheckpoint("checkpoint-2");
        partition.WriteBlocks(8, 'c');
        partition.WriteBlocks(11, 'B');
        partition.WriteBlocks(12, 'C');
        partition.WriteBlocks(15, 'F');

        ui32 promoteAddBlobsRequestCount = 0;
        TMap<ui32, TLogoBlobID> l0BlobIdsByRange;
        TMap<ui32, TLogoBlobID> l1BlobIdsByRange;
        TVector<TVector<ui32>> l1BlobBlockIndices;
        TVector<TVector<ui64>> l1BlobCommitIds;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* request =
                        event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();

                    if (request->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                        for (const auto& blob: request->L0Blobs) {
                            UNIT_ASSERT(!blob.BlockIndices.empty());
                            const ui32 rangeIndex =
                                blob.BlockIndices.front() / L0RangeBlockCount;
                            const bool inserted =
                                l0BlobIdsByRange
                                    .emplace(
                                        rangeIndex,
                                        MakeBlobId(TestTabletId, blob.BlobId))
                                    .second;
                            UNIT_ASSERT(inserted);
                        }
                    } else if (
                        request->Mode ==
                        EAddBlobMode::ADD_PROMOTE_COMPACTION_RESULT)
                    {
                        ++promoteAddBlobsRequestCount;

                        UNIT_ASSERT(request->MixedBlobs.empty());
                        UNIT_ASSERT(request->MergedBlobs.empty());
                        UNIT_ASSERT(request->FreshBlobs.empty());
                        UNIT_ASSERT(request->L0Blobs.empty());
                        UNIT_ASSERT_VALUES_EQUAL(
                            1,
                            request->AffectedBlobs.size());

                        const auto sourceBlobId =
                            l0BlobIdsByRange.find(PromotedRangeIndex);
                        UNIT_ASSERT(sourceBlobId != l0BlobIdsByRange.end());
                        UNIT_ASSERT_VALUES_EQUAL(
                            sourceBlobId->second,
                            MakeBlobId(
                                TestTabletId,
                                request->AffectedBlobs.begin()->first));

                        for (const auto& blob: request->L1Blobs) {
                            l1BlobBlockIndices.push_back(blob.BlockIndices);
                            l1BlobCommitIds.push_back(blob.CommitIds);

                            UNIT_ASSERT(!blob.BlockIndices.empty());
                            const ui32 rangeIndex =
                                blob.BlockIndices.front() / L1RangeBlockCount;
                            UNIT_ASSERT_VALUES_EQUAL(
                                rangeIndex,
                                blob.BlockIndices.back() / L1RangeBlockCount);
                            const bool inserted =
                                l1BlobIdsByRange
                                    .emplace(
                                        rangeIndex,
                                        MakeBlobId(TestTabletId, blob.BlobId))
                                    .second;
                            UNIT_ASSERT(inserted);
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(2, l0BlobIdsByRange.size());

        auto request = std::make_unique<
            TEvPartitionPrivate::TEvPromoteCompactionRequest>();
        request->RangeIndex = PromotedRangeIndex;
        partition.SendToPipe(std::move(request));

        const auto response = partition.RecvResponse<
            TEvPartitionPrivate::TEvPromoteCompactionResponse>();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        UNIT_ASSERT_VALUES_EQUAL(1, promoteAddBlobsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(2, l1BlobBlockIndices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            l1BlobBlockIndices.size(),
            l1BlobCommitIds.size());
        UNIT_ASSERT_VALUES_EQUAL(2, l1BlobIdsByRange.size());

        TVector<ui32> promotedBlockIndices;
        for (size_t i = 0; i < l1BlobBlockIndices.size(); ++i) {
            const auto& blockIndices = l1BlobBlockIndices[i];
            const auto& commitIds = l1BlobCommitIds[i];

            UNIT_ASSERT_VALUES_EQUAL(blockIndices.size(), commitIds.size());
            for (ui64 commitId: commitIds) {
                UNIT_ASSERT(commitId != 0);
            }

            promotedBlockIndices.insert(
                promotedBlockIndices.end(),
                blockIndices.begin(),
                blockIndices.end());
        }

        Sort(promotedBlockIndices);
        const TVector<ui32> expectedPromotedBlockIndices = {8, 11, 12, 15};
        UNIT_ASSERT_VALUES_EQUAL(
            expectedPromotedBlockIndices,
            promotedBlockIndices);

        const auto unpromotedBlobId = l0BlobIdsByRange.find(0);
        UNIT_ASSERT(unpromotedBlobId != l0BlobIdsByRange.end());
        AssertDescribeBlockContent(
            partition,
            0,
            {},
            unpromotedBlobId->second,
            '0');

        const auto promotedSourceBlobId =
            l0BlobIdsByRange.find(PromotedRangeIndex);
        UNIT_ASSERT(promotedSourceBlobId != l0BlobIdsByRange.end());
        AssertDescribeBlockContent(
            partition,
            8,
            "checkpoint-1",
            promotedSourceBlobId->second,
            'a');
        AssertDescribeBlockContent(
            partition,
            8,
            "checkpoint-2",
            promotedSourceBlobId->second,
            'b');

        const auto promotedBlobId =
            l1BlobIdsByRange.find(8 / L1RangeBlockCount);
        UNIT_ASSERT(promotedBlobId != l1BlobIdsByRange.end());
        AssertDescribeBlockContent(
            partition,
            8,
            {},
            promotedBlobId->second,
            'c');
    }

    Y_UNIT_TEST(ShouldTriggerPromoteCompactionByUsedBlocksPerRange)
    {
        constexpr ui32 L0RangeBlockCount = 8;
        constexpr ui32 L1RangeBlockCount = 4;
        constexpr ui32 BlocksForHugeBlob = 2;
        constexpr ui32 UsedBlocksNeededForPromote =
            BlocksForHugeBlob * L0RangeBlockCount / L1RangeBlockCount;
        constexpr ui32 BlockCount = 2 * L0RangeBlockCount;

        static_assert(UsedBlocksNeededForPromote == 4);

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWriteBlobThresholdSSD(
            BlocksForHugeBlob * DefaultBlockSize);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 l0PromoteRequestCount = 0;
        ui32 l1PromoteRequestCount = 0;
        ui32 l0PromoteAddBlobsRequestCount = 0;
        ui32 l1PromoteAddBlobsRequestCount = 0;
        ui32 l0PromoteCompletedCount = 0;
        ui32 l1PromoteCompletedCount = 0;
        TVector<std::pair<TPartialBlobId, ui32>> l0BlobRanges;
        TVector<ui32> promotedBlockIndices;
        ui64 maxPromotedBlockCommitId = 0;
        ui64 mergedCommitId = 0;
        TLogoBlobID mergedBlobId;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvPromoteCompactionRequest: {
                        const auto* request = event->Get<
                            TEvPartitionPrivate::TEvPromoteCompactionRequest>();
                        UNIT_ASSERT(!request->RangeIndex);
                        if (request->Source ==
                            EPromoteCompactionSource::L0)
                        {
                            ++l0PromoteRequestCount;
                        } else {
                            UNIT_ASSERT(
                                request->Source ==
                                EPromoteCompactionSource::L1);
                            ++l1PromoteRequestCount;
                        }
                        break;
                    }

                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        const auto* request = event->Get<
                            TEvPartitionPrivate::TEvAddBlobsRequest>();

                        if (request->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                            for (const auto& blob: request->L0Blobs) {
                                UNIT_ASSERT(!blob.BlockIndices.empty());
                                const ui32 rangeIndex =
                                    blob.BlockIndices.front() /
                                    L0RangeBlockCount;
                                l0BlobRanges.emplace_back(
                                    blob.BlobId,
                                    rangeIndex);
                            }
                        } else if (
                            request->Mode ==
                            EAddBlobMode::ADD_PROMOTE_COMPACTION_RESULT)
                        {
                            if (!request->L1Blobs.empty()) {
                                ++l0PromoteAddBlobsRequestCount;
                                UNIT_ASSERT(request->MergedBlobs.empty());
                                UNIT_ASSERT_VALUES_EQUAL(
                                    2,
                                    request->AffectedBlobs.size());
                                for (const auto& [blobId, affectedBlob]:
                                     request->AffectedBlobs)
                                {
                                    Y_UNUSED(affectedBlob);

                                    bool sourceBlobFound = false;
                                    for (const auto& [sourceBlobId, rangeIndex]:
                                         l0BlobRanges)
                                    {
                                        if (sourceBlobId == blobId) {
                                            UNIT_ASSERT_VALUES_EQUAL(
                                                0,
                                                rangeIndex);
                                            sourceBlobFound = true;
                                            break;
                                        }
                                    }
                                    UNIT_ASSERT(sourceBlobFound);
                                }

                                UNIT_ASSERT_VALUES_EQUAL(
                                    1,
                                    request->L1Blobs.size());
                                promotedBlockIndices =
                                    request->L1Blobs.front().BlockIndices;
                                for (ui64 commitId:
                                     request->L1Blobs.front().CommitIds)
                                {
                                    maxPromotedBlockCommitId = Max(
                                        maxPromotedBlockCommitId,
                                        commitId);
                                }
                            } else {
                                ++l1PromoteAddBlobsRequestCount;
                                UNIT_ASSERT_VALUES_EQUAL(
                                    1,
                                    request->AffectedBlobs.size());
                                UNIT_ASSERT_VALUES_EQUAL(
                                    1,
                                    request->MergedBlobs.size());

                                const auto& blob =
                                    request->MergedBlobs.front();
                                UNIT_ASSERT_VALUES_EQUAL(
                                    TBlockRange32::MakeClosedInterval(0, 3),
                                    blob.BlockRange);
                                mergedCommitId = blob.CommitId;
                                mergedBlobId = MakeBlobId(
                                    TestTabletId,
                                    blob.BlobId);
                            }
                        }
                        break;
                    }

                    case TEvPartitionPrivate::EvPromoteCompactionCompleted: {
                        const auto* completed = event->Get<
                            TEvPartitionPrivate::
                                TEvPromoteCompactionCompleted>();
                        if (completed->Source ==
                            EPromoteCompactionSource::L0)
                        {
                            ++l0PromoteCompletedCount;
                        } else {
                            UNIT_ASSERT(
                                completed->Source ==
                                EPromoteCompactionSource::L1);
                            ++l1PromoteCompletedCount;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (ui32 blockIndex: {0, 1, 2, 8, 9, 10}) {
            partition.WriteBlocks(blockIndex, 'a');
        }
        partition.Flush();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(0, l0PromoteRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l1PromoteRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l0PromoteAddBlobsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l1PromoteAddBlobsRequestCount);

        partition.WriteBlocks(3, 'b');
        partition.Flush();

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return l0PromoteCompletedCount == 1 &&
                   l1PromoteCompletedCount == 1;
        };
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, l0PromoteRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l1PromoteRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l0PromoteAddBlobsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l1PromoteAddBlobsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l0PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l1PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>({0, 1, 2, 3}),
            promotedBlockIndices);
        UNIT_ASSERT_VALUES_EQUAL(maxPromotedBlockCommitId, mergedCommitId);

        AssertDescribeBlockContent(partition, 0, {}, mergedBlobId, 'a');
        AssertDescribeBlockContent(partition, 3, {}, mergedBlobId, 'b');
    }

    Y_UNIT_TEST(ShouldPromoteSpecifiedL1RangeToFixedSizeMergedRanges)
    {
        constexpr ui32 MergedRangeBlockCount = 4_MB / DefaultBlockSize;
        constexpr ui32 L1RangeBlockCount = 2 * MergedRangeBlockCount;
        constexpr ui32 L0RangeBlockCount = 2 * L1RangeBlockCount;
        constexpr ui32 BlockCount = L1RangeBlockCount;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWriteBlobThresholdSSD(16_MB);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 maxPromotedBlockCommitId = 0;
        TVector<TBlockRange32> mergedRanges;
        TVector<ui64> mergedCommitIds;
        TMap<ui32, TLogoBlobID> mergedBlobIds;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* request = event->Get<
                        TEvPartitionPrivate::TEvAddBlobsRequest>();
                    if (request->Mode !=
                        EAddBlobMode::ADD_PROMOTE_COMPACTION_RESULT)
                    {
                        return TTestActorRuntime::DefaultObserverFunc(event);
                    }

                    if (!request->L1Blobs.empty()) {
                        for (const auto& blob: request->L1Blobs) {
                            for (ui64 commitId: blob.CommitIds) {
                                maxPromotedBlockCommitId = Max(
                                    maxPromotedBlockCommitId,
                                    commitId);
                            }
                        }
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(
                            1,
                            request->AffectedBlobs.size());
                        UNIT_ASSERT_VALUES_EQUAL(
                            2,
                            request->MergedBlobs.size());

                        for (const auto& blob: request->MergedBlobs) {
                            const ui32 rangeIndex =
                                blob.BlockRange.Start /
                                MergedRangeBlockCount;
                            UNIT_ASSERT_VALUES_EQUAL(
                                rangeIndex * MergedRangeBlockCount,
                                blob.BlockRange.Start);
                            UNIT_ASSERT_VALUES_EQUAL(
                                (rangeIndex + 1) * MergedRangeBlockCount - 1,
                                blob.BlockRange.End);
                            UNIT_ASSERT_VALUES_UNEQUAL(
                                blob.BlobId.CommitId(),
                                blob.CommitId);

                            mergedRanges.push_back(blob.BlockRange);
                            mergedCommitIds.push_back(blob.CommitId);
                            mergedBlobIds.emplace(
                                rangeIndex,
                                MakeBlobId(TestTabletId, blob.BlobId));
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.WriteBlocks(0, '0');
        partition.WriteBlocks(MergedRangeBlockCount - 1, 'a');
        partition.WriteBlocks(MergedRangeBlockCount, 'b');
        partition.WriteBlocks(BlockCount - 1, 'c');
        partition.Flush();

        auto l0Request = std::make_unique<
            TEvPartitionPrivate::TEvPromoteCompactionRequest>();
        l0Request->RangeIndex = 0;
        partition.SendToPipe(std::move(l0Request));
        auto l0Response = partition.RecvResponse<
            TEvPartitionPrivate::TEvPromoteCompactionResponse>();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, l0Response->GetStatus());

        auto l1Request = std::make_unique<
            TEvPartitionPrivate::TEvPromoteCompactionRequest>();
        l1Request->Source = EPromoteCompactionSource::L1;
        l1Request->RangeIndex = 0;
        partition.SendToPipe(std::move(l1Request));
        auto l1Response = partition.RecvResponse<
            TEvPartitionPrivate::TEvPromoteCompactionResponse>();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, l1Response->GetStatus());

        Sort(
            mergedRanges.begin(),
            mergedRanges.end(),
            [](const auto& lhs, const auto& rhs)
            {
                return lhs.Start < rhs.Start;
            });

        UNIT_ASSERT_VALUES_EQUAL(2, mergedRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange32::MakeClosedInterval(
                0,
                MergedRangeBlockCount - 1),
            mergedRanges[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange32::MakeClosedInterval(
                MergedRangeBlockCount,
                BlockCount - 1),
            mergedRanges[1]);
        UNIT_ASSERT_VALUES_EQUAL(2, mergedCommitIds.size());
        UNIT_ASSERT_VALUES_EQUAL(
            maxPromotedBlockCommitId,
            mergedCommitIds[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            maxPromotedBlockCommitId,
            mergedCommitIds[1]);

        AssertDescribeBlockContent(
            partition,
            0,
            {},
            mergedBlobIds.at(0),
            '0');
        AssertDescribeBlockContent(
            partition,
            MergedRangeBlockCount - 1,
            {},
            mergedBlobIds.at(0),
            'a');
        AssertDescribeBlockContent(
            partition,
            MergedRangeBlockCount,
            {},
            mergedBlobIds.at(1),
            'b');
        AssertDescribeBlockContent(
            partition,
            BlockCount - 1,
            {},
            mergedBlobIds.at(1),
            'c');
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
