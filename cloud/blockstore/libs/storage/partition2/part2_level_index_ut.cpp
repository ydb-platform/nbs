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

void AssertReadBlockContent(
    TPartitionClient& partition,
    ui32 blockIndex,
    char expectedContent)
{
    const auto response = partition.ReadBlocks(blockIndex);

    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetBlocks().BuffersSize());
    UNIT_ASSERT_VALUES_EQUAL(
        TString(DefaultBlockSize, expectedContent),
        response->Record.GetBlocks().GetBuffers(0));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition2LevelIndexTest)
{
    Y_UNIT_TEST(ShouldFlushFreshBlocksToL0Index)
    {
        constexpr ui32 L1RangeBlockCount = MaxBlocksCount;
        constexpr ui32 L0RangeBlockCount = 2 * L1RangeBlockCount;
        constexpr ui32 BlockCount = 3 * L0RangeBlockCount;
        constexpr ui32 VersionedBlockIndex = L0RangeBlockCount - 1;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '0');
        partition.WriteBlocks(VersionedBlockIndex, 'a');
        partition.CreateCheckpoint("checkpoint-1");
        partition.WriteBlocks(VersionedBlockIndex, 'b');
        partition.CreateCheckpoint("checkpoint-2");
        partition.WriteBlocks(VersionedBlockIndex, 'c');
        partition.WriteBlocks(L0RangeBlockCount, '4');
        partition.WriteBlocks(2 * L0RangeBlockCount - 1, '7');
        partition.WriteBlocks(2 * L0RangeBlockCount, '8');
        partition.WriteBlocks(3 * L0RangeBlockCount - 1, 'B');

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
            VersionedBlockIndex,
            VersionedBlockIndex,
            VersionedBlockIndex,
            L0RangeBlockCount,
            2 * L0RangeBlockCount - 1,
            2 * L0RangeBlockCount,
            3 * L0RangeBlockCount - 1,
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
            VersionedBlockIndex,
            L0RangeBlockCount,
            2 * L0RangeBlockCount - 1,
            2 * L0RangeBlockCount,
            3 * L0RangeBlockCount - 1,
        };
        UNIT_ASSERT_VALUES_EQUAL(
            expectedDescribedBlockIndices,
            describedBlockIndices);

        const auto versionedBlockBlobId = l0BlobIdsByRange.find(0);
        UNIT_ASSERT(versionedBlockBlobId != l0BlobIdsByRange.end());

        AssertDescribeBlockContent(
            partition,
            VersionedBlockIndex,
            "checkpoint-1",
            versionedBlockBlobId->second,
            'a');
        AssertDescribeBlockContent(
            partition,
            VersionedBlockIndex,
            "checkpoint-2",
            versionedBlockBlobId->second,
            'b');
        AssertDescribeBlockContent(
            partition,
            VersionedBlockIndex,
            {},
            versionedBlockBlobId->second,
            'c');
    }

    Y_UNIT_TEST(ShouldPromoteSpecifiedL0RangeToL1Index)
    {
        constexpr ui32 L1RangeBlockCount = MaxBlocksCount;
        constexpr ui32 L0RangeBlockCount = 2 * L1RangeBlockCount;
        constexpr ui32 PromotedRangeIndex = 1;
        constexpr ui32 BlockCount = 2 * L0RangeBlockCount;
        constexpr ui32 PromotedRangeStart =
            PromotedRangeIndex * L0RangeBlockCount;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, '0');
        partition.WriteBlocks(L0RangeBlockCount - 1, '7');
        partition.WriteBlocks(PromotedRangeStart, 'a');
        partition.CreateCheckpoint("checkpoint-1");
        partition.WriteBlocks(PromotedRangeStart, 'b');
        partition.CreateCheckpoint("checkpoint-2");
        partition.WriteBlocks(PromotedRangeStart, 'c');
        partition.WriteBlocks(
            PromotedRangeStart + L1RangeBlockCount - 1,
            'B');
        partition.WriteBlocks(
            PromotedRangeStart + L1RangeBlockCount,
            'C');
        partition.WriteBlocks(BlockCount - 1, 'F');

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
        const TVector<ui32> expectedPromotedBlockIndices = {
            PromotedRangeStart,
            PromotedRangeStart + L1RangeBlockCount - 1,
            PromotedRangeStart + L1RangeBlockCount,
            BlockCount - 1,
        };
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
            PromotedRangeStart,
            "checkpoint-1",
            promotedSourceBlobId->second,
            'a');
        AssertDescribeBlockContent(
            partition,
            PromotedRangeStart,
            "checkpoint-2",
            promotedSourceBlobId->second,
            'b');

        const auto promotedBlobId =
            l1BlobIdsByRange.find(PromotedRangeStart / L1RangeBlockCount);
        UNIT_ASSERT(promotedBlobId != l1BlobIdsByRange.end());
        AssertDescribeBlockContent(
            partition,
            PromotedRangeStart,
            {},
            promotedBlobId->second,
            'c');
    }

    Y_UNIT_TEST(ShouldTriggerPromoteCompactionByUsedBlocksPerRange)
    {
        constexpr ui32 MergedRangeBlockCount = MaxBlocksCount;
        constexpr ui32 L1RangeBlockCount = 2 * MergedRangeBlockCount;
        constexpr ui32 L0RangeBlockCount = 2 * L1RangeBlockCount;
        constexpr ui32 BlocksForHugeBlob = 2;
        constexpr ui32 UsedBlocksNeededForL0Promote =
            BlocksForHugeBlob * L0RangeBlockCount / L1RangeBlockCount;
        constexpr ui32 UsedBlocksNeededForL1Promote =
            BlocksForHugeBlob * L1RangeBlockCount / MergedRangeBlockCount;
        constexpr ui32 BlockCount = L0RangeBlockCount;

        static_assert(UsedBlocksNeededForL0Promote == 4);
        static_assert(UsedBlocksNeededForL1Promote == 4);

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWriteBlobThresholdSSD(
            BlocksForHugeBlob * DefaultBlockSize);
        config.SetCleanupThreshold(1);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, BlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 l0PromoteCompletedCount = 0;
        ui32 l1PromoteCompletedCount = 0;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvPromoteCompactionCompleted)
                {
                    const auto* completed = event->Get<
                        TEvPartitionPrivate::TEvPromoteCompactionCompleted>();
                    UNIT_ASSERT_C(
                        !HasError(completed->GetError()),
                        FormatError(completed->GetError()));

                    if (completed->Source == EPromoteCompactionSource::L0) {
                        ++l0PromoteCompletedCount;
                    } else {
                        UNIT_ASSERT(
                            completed->Source ==
                            EPromoteCompactionSource::L1);
                        ++l1PromoteCompletedCount;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto waitForCompactions = [&](ui32 l0Count, ui32 l1Count) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return l0PromoteCompletedCount == l0Count &&
                       l1PromoteCompletedCount == l1Count;
            };
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        };

        partition.WriteBlocks(0, 'a');
        partition.WriteBlocks(1, 'a');
        partition.WriteBlocks(L1RangeBlockCount, 'a');
        partition.Flush();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0, l0PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l1PromoteCompletedCount);

        // The fourth used block must trigger promotion using the three
        // counters restored from the local DB.
        partition.RebootTablet();
        partition.WaitReady();

        partition.WriteBlocks(L1RangeBlockCount + 1, 'b');
        partition.Flush();

        waitForCompactions(1, 0);
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, l0PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l1PromoteCompletedCount);

        // Restore the completed L0 range and the L1 result. The next L0
        // promotion must start from an empty L0 range and add to the existing
        // L1 filter and compaction counters.
        partition.RebootTablet();
        partition.WaitReady();

        partition.WriteBlocks(2, 'c');
        partition.WriteBlocks(3, 'c');
        partition.WriteBlocks(4, 'c');
        partition.Flush();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, l0PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(0, l1PromoteCompletedCount);

        partition.WriteBlocks(5, 'd');
        partition.Flush();

        waitForCompactions(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, l0PromoteCompletedCount);
        UNIT_ASSERT_VALUES_EQUAL(1, l1PromoteCompletedCount);
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

    Y_UNIT_TEST(ShouldPreserveNewerL0BlocksDuringMergedRangeCompaction)
    {
        constexpr ui32 MergedRangeBlockCount = MaxBlocksCount;
        constexpr ui32 L1RangeBlockCount = 2 * MergedRangeBlockCount;
        constexpr ui32 L0RangeBlockCount = 2 * L1RangeBlockCount;

        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);
        config.SetWriteBlobThresholdSSD(16_MB);
        config.SetL0RangeSizeV2(L0RangeBlockCount * DefaultBlockSize);
        config.SetL1RangeSizeV2(L1RangeBlockCount * DefaultBlockSize);

        auto runtime = PrepareTestActorRuntime(config, L1RangeBlockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 promotedCommitId = 0;
        ui64 newerL0CommitId = 0;
        ui64 compactedCommitId = 0;
        ui32 flushCount = 0;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionPrivate::EvAddBlobsRequest)
                {
                    const auto* request = event->Get<
                        TEvPartitionPrivate::TEvAddBlobsRequest>();

                    if (request->Mode == EAddBlobMode::ADD_FLUSH_RESULT) {
                        ++flushCount;
                        if (flushCount == 2) {
                            for (const auto& blob: request->L0Blobs) {
                                for (ui64 commitId: blob.CommitIds) {
                                    newerL0CommitId = Max(
                                        newerL0CommitId,
                                        commitId);
                                }
                            }
                        }
                    } else if (
                        request->Mode ==
                            EAddBlobMode::ADD_PROMOTE_COMPACTION_RESULT &&
                        !request->MergedBlobs.empty())
                    {
                        for (const auto& blob: request->MergedBlobs) {
                            promotedCommitId = Max(
                                promotedCommitId,
                                blob.CommitId);
                        }
                    } else if (
                        request->Mode ==
                        EAddBlobMode::ADD_COMPACTION_RESULT)
                    {
                        UNIT_ASSERT_VALUES_EQUAL(
                            1,
                            request->MergedBlobs.size());
                        compactedCommitId =
                            request->MergedBlobs.front().CommitId;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.WriteBlocks(0, 'a');
        partition.WriteBlocks(1, 's');
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

        partition.WriteBlocks(0, 'b');
        partition.Flush();
        partition.Compaction();

        UNIT_ASSERT(promotedCommitId);
        UNIT_ASSERT(newerL0CommitId);
        UNIT_ASSERT(compactedCommitId);
        UNIT_ASSERT_VALUES_EQUAL(promotedCommitId, compactedCommitId);
        UNIT_ASSERT(compactedCommitId < newerL0CommitId);

        AssertReadBlockContent(partition, 0, 'b');
        AssertReadBlockContent(partition, 1, 's');

        partition.Cleanup();

        AssertReadBlockContent(partition, 0, 'b');
        AssertReadBlockContent(partition, 1, 's');
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
