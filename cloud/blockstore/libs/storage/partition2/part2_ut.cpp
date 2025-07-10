#include "part2.h"

#include "part2_events_private.h"
#include "part2_schema.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition2.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/bitmap.h>
#include <util/generic/size_literals.h>
#include <util/generic/variant.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestPartitionInfo
{
    TString DiskId = "test";
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    ui64 TabletId = TestTabletId;
    ui64 BaseTabletId = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
};

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final
    : public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVV");

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

TString GetBlocksContent(
    char fill = 0,
    ui32 blocksCount = 1,
    size_t blockSize = DefaultBlockSize)
{
    TString result;
    for (ui32 i = 0; i < blocksCount; ++i) {
        result += GetBlockContent(fill, blockSize);
    }
    return result;
}

void CheckRangesArePartition(
    TVector<TBlockRange32> ranges,
    const TBlockRange32& unionRange)
{
    UNIT_ASSERT(ranges.size() > 0);

    Sort(ranges, [](const auto& l, const auto& r) {
            return l.Start < r.Start;
        });

    const auto* prev = &ranges.front();
    UNIT_ASSERT_VALUES_EQUAL(unionRange.Start, prev->Start);

    for (size_t i = 1; i < ranges.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(prev->End + 1, ranges[i].Start,
            "during iteration #" << i);
        prev = &ranges[i];
    }

    UNIT_ASSERT_VALUES_EQUAL(unionRange.End, ranges.back().End);
}

NProto::TStorageServiceConfig DefaultConfig(ui32 flushBlobSizeThreshold = 4_KB)
{
    NProto::TStorageServiceConfig config;
    config.SetFlushBlobSizeThreshold(flushBlobSizeThreshold);
    config.SetFreshByteCountThresholdForBackpressure(400_KB);
    config.SetFreshByteCountLimitForBackpressure(1200_KB);
    config.SetFreshByteCountFeatureMaxValue(6);
    config.SetCollectGarbageThreshold(10);

    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

////////////////////////////////////////////////////////////////////////////////

void InitTestActorRuntime(
    TTestActorRuntime& runtime,
    const NProto::TStorageServiceConfig& config,
    ui32 blocksCount,
    ui32 channelCount,
    std::unique_ptr<TTabletStorageInfo> tabletInfo,
    TTestPartitionInfo partitionInfo = {},
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default,
    ui32 zoneBlockCount = 32 * MaxBlocksCount)
{
    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );

    NProto::TPartitionConfig partConfig;
    partConfig.SetDiskId(partitionInfo.DiskId);
    partConfig.SetBaseDiskId(partitionInfo.BaseDiskId);
    partConfig.SetBaseDiskCheckpointId(partitionInfo.BaseDiskCheckpointId);
    partConfig.SetStorageMediaKind(partitionInfo.MediaKind);

    partConfig.SetBlockSize(DefaultBlockSize);
    partConfig.SetBlocksCount(blocksCount);

    auto* cps = partConfig.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount - DataChannelOffset - 1; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    partConfig.SetZoneBlockCount(zoneBlockCount);

    partConfig.SetTabletVersion(2);

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            auto tablet = CreatePartitionTablet(
                owner,
                info,
                storageConfig,
                diagConfig,
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                partConfig,
                storageAccessMode,
                1,  // siblingCount
                VolumeActorId,
                0  // volumeTabletId
            );
            return tablet.release();
        };

    auto bootstrapper =
        CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(bootstrapper);
}

////////////////////////////////////////////////////////////////////////////////

auto InitTestActorRuntime(
    TTestEnv& env,
    TTestActorRuntime& runtime,
    ui32 channelCount,
    ui32 tabletInfoChannelCount,
    const NProto::TStorageServiceConfig& config = DefaultConfig())
{
    {
        env.CreateSubDomain("nbs");
        auto storageConfig = CreateTestStorageConfig(config);
        env.CreateBlockStoreNode(
            "nbs",
            storageConfig,
            CreateTestDiagnosticsConfig()
        );
    }

    const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
    std::unique_ptr<TTabletStorageInfo> x(new TTabletStorageInfo());

    x->TabletID = tabletId;
    x->TabletType = TTabletTypes::BlockStorePartition;
    auto& channels = x->Channels;
    channels.resize(tabletInfoChannelCount);

    for (ui64 channel = 0; channel < channels.size(); ++channel) {
        channels[channel].Channel = channel;
        channels[channel].Type =
            TBlobStorageGroupType(BootGroupErasure);
        channels[channel].History.resize(1);
        channels[channel].History[0].FromGeneration = 0;
        const auto gidx =
            channel > DataChannelOffset ? channel - DataChannelOffset : 0;
        channels[channel].History[0].GroupID = env.GetGroupIds()[gidx];
    }

    InitTestActorRuntime(runtime, config, 1024, channelCount, std::move(x));

    return tabletId;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    const NProto::TStorageServiceConfig& config = DefaultConfig(),
    ui32 blocksCount = 1024,
    TMaybe<ui32> channelsCount = {},
    const TTestPartitionInfo& testPartitionInfo = {},
    IActorPtr volumeProxy = {},
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default,
    ui32 zoneBlockCount = 32 * MaxBlocksCount)
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    if (volumeProxy) {
        runtime->AddLocalService(
            MakeVolumeProxyServiceId(),
            TActorSetupCmd(volumeProxy.release(), TMailboxType::Simple, 0));
    }

    runtime->AddLocalService(
        VolumeActorId,
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0)
    );

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
    //     runtime->SetLogPriority(i, NLog::PRI_DEBUG);
    // }
    // runtime->SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    runtime->SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);

    SetupTabletServices(*runtime);

    std::unique_ptr<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(
        testPartitionInfo.TabletId,
        TTabletTypes::BlockStorePartition2));

    if (channelsCount) {
        auto& channels = tabletInfo->Channels;
        channels.resize(*channelsCount);
        for (ui64 i = 0; i < channels.size(); ++i) {
            auto& channel = channels[i];
            channel.History.resize(1);
        }
    }

    InitTestActorRuntime(
        *runtime,
        config,
        blocksCount,
        channelsCount ? *channelsCount : tabletInfo->Channels.size(),
        std::move(tabletInfo),
        testPartitionInfo,
        storageAccessMode,
        zoneBlockCount);

    return runtime;
}

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    if (response->Record.GetBlocks().BuffersSize() == 1) {
        return response->Record.GetBlocks().GetBuffers(0);
    }
    return {};
}

TString GetBlocksContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    const auto& blocks = response->Record.GetBlocks();

    {
        bool empty = true;
        for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
            if (blocks.GetBuffers(i)) {
                empty = false;
            }
        }

        if (empty) {
            return {};
        }
    }

    TString result;

    for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
        const auto& block = blocks.GetBuffers(i);
        if (!block) {
            result += GetBlockContent(char(0));
            continue;
        }
        result += block;
    }

    return result;
}

template <typename T>
void AssertEqual(const TVector<T>& l, const TVector<T>& r)
{
    UNIT_ASSERT_EQUAL(l.size(), r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        UNIT_ASSERT_EQUAL(l[i], r[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDynBitMap GetBitMap(const TString& s)
{
    TDynBitMap mask;

    if (s) {
        mask.Reserve(s.size() * 8);
        Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TDynBitMap::TChunk) == s.size());
        auto* dst = const_cast<TDynBitMap::TChunk*>(mask.GetChunks());
        memcpy(dst, s.data(), s.size());
    }

    return mask;
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksLocalResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap CreateBitmap(size_t size)
{
    TDynBitMap bitmap;
    bitmap.Reserve(size);
    bitmap.Set(0, size);
    return bitmap;
}

void MarkZeroedBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Set(range.Start, range.End + 1);
}

void MarkWrittenBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Reset(range.Start, range.End + 1);
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    ui64 TabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TPartitionClient(
            TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 tabletId = TestTabletId)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , TabletId(tabletId)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    void RebootTablet()
    {
        TVector<ui64> tablets = { TabletId };
        auto guard = CreateTabletScheduledEventsGuard(
            tablets,
            Runtime,
            Sender);

        NKikimr::RebootTablet(Runtime, TabletId, Sender);

        // sooner or later after reset pipe will reconnect
        // but we do not want to wait
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    void KillTablet()
    {
        SendToPipe(std::make_unique<TEvents::TEvPoisonPill>());
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }

    template <typename TRequest>
    void SendToPipe(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime.SendToPipe(
            PipeClient,
            Sender,
            request.release(),
            NodeIdx,
            cookie);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT_C(handle, TypeName<TResponse>() << " is expected");
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvPartition::TEvStatPartitionRequest> CreateStatPartitionRequest()
    {
        return std::make_unique<TEvPartition::TEvStatPartitionRequest>();
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        TString blockContent)
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        *request->Record.MutableBlocks()->MutableBuffers()->Add() = blockContent;
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange32& writeRange,
        char fill = 0)
    {
        auto blockContent = GetBlockContent(fill);

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(writeRange.Start);

        auto& buffers = *request->Record.MutableBlocks()->MutableBuffers();
        for (ui32 i = 0; i < writeRange.Size(); ++i) {
            *buffers.Add() = blockContent;
        }

        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        const TBlockRange32& writeRange,
        TStringBuf blockContent)
    {
        TSgList sglist;
        sglist.resize(writeRange.Size(), {blockContent.data(), blockContent.size()});

        auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        request->Record.SetStartIndex(writeRange.Start);
        request->Record.Sglist = TGuardedSgList(std::move(sglist));
        request->Record.BlocksCount = writeRange.Size();
        request->Record.BlockSize = blockContent.size() / writeRange.Size();
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        char fill = 0)
    {
        return CreateWriteBlocksRequest(blockIndex, GetBlockContent(fill));
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        ui32 blockIndex,
        TStringBuf blockContent)
    {
        return CreateWriteBlocksLocalRequest(
            TBlockRange32::MakeOneBlock(blockIndex),
            std::move(blockContent));
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        ui32 blockIndex)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        return request;
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        const TBlockRange32& writeRange)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(writeRange.Start);
        request->Record.SetBlocksCount(writeRange.Size());
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        ui32 blockIndex,
        const TString& checkpointId = {})
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        const TBlockRange32 blockRange,
        const TString& checkpointId = {})
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(blockRange.Start);
        request->Record.SetBlocksCount(blockRange.Size());
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        ui32 blockIndex,
        TStringBuf buffer,
        const TString& checkpointId = {})
    {
        return CreateReadBlocksLocalRequest(
            TBlockRange32::MakeOneBlock(blockIndex),
            {TBlockDataRef(buffer.data(), buffer.size())},
            checkpointId);
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        const TBlockRange32& readRange,
        const TSgList& sglist,
        const TString& checkpointId = {})
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetStartIndex(readRange.Start);
        request->Record.SetBlocksCount(readRange.Size());

        request->Record.Sglist = TGuardedSgList(sglist);
        request->Record.BlockSize = SgListGetSize(sglist) / readRange.Size();
        return request;
    }

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest> CreateCreateCheckpointRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> CreateDeleteCheckpointRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest> CreateDeleteCheckpointDataRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvVolume::TEvDeleteCheckpointDataRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvGetChangedBlocksRequest> CreateGetChangedBlocksRequest(
        const TBlockRange32& range,
        const TString& lowCheckpointId,
        const TString& highCheckpointId)
    {
        auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();
        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlocksCount(range.Size());
        request->Record.SetLowCheckpointId(lowCheckpointId);
        request->Record.SetHighCheckpointId(highCheckpointId);
        return request;
    }

    std::unique_ptr<TEvPartition::TEvWaitReadyRequest> CreateWaitReadyRequest()
    {
        return std::make_unique<TEvPartition::TEvWaitReadyRequest>();
    }

    std::unique_ptr<TEvPartitionPrivate::TEvFlushRequest> CreateFlushRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvFlushRequest>();
    }

    std::unique_ptr<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest> CreateTrimFreshLogRequest()
    {
        return std::make_unique<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest>();
    }

    template <typename... TArgs>
    std::unique_ptr<TEvPartitionPrivate::TEvCompactionRequest> CreateCompactionRequest(TArgs&&... args)
    {
        return std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(std::forward<TArgs>(args)...);
    }

    std::unique_ptr<TEvPartitionPrivate::TEvCompactionRequest> CreateCompactionRequest(
        TEvPartitionPrivate::ECompactionMode mode,
        TVector<TPartialBlobId> blobIds)
    {
        auto request =
            std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(mode);
        for (const auto& blobId: blobIds) {
            request->GarbageInfo.BlobCounters.push_back({blobId, 0});
        }
        return request;
    }

    std::unique_ptr<TEvPartitionPrivate::TEvCleanupRequest> CreateCleanupRequest(
        TEvPartitionPrivate::ECleanupMode mode = TEvPartitionPrivate::DirtyBlobCleanup)
    {
        return std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>(mode);
    }

    std::unique_ptr<TEvPartitionPrivate::TEvUpdateIndexStructuresRequest> CreateUpdateIndexStructuresRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvUpdateIndexStructuresRequest>();
    }

    std::unique_ptr<TEvTablet::TEvGetCounters> CreateGetCountersRequest()
    {
        return std::make_unique<TEvTablet::TEvGetCounters>();
    }

    std::unique_ptr<TEvPartitionPrivate::TEvCollectGarbageRequest> CreateCollectGarbageRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();
    }

    std::unique_ptr<TEvPartitionPrivate::TEvDeleteGarbageRequest> CreateDeleteGarbageRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvDeleteGarbageRequest>();
    }

    void SendGetCountersRequest()
    {
        SendToPipe(CreateGetCountersRequest());
    }

    std::unique_ptr<TEvTablet::TEvGetCountersResponse> RecvGetCountersResponse()
    {
        return RecvResponse<TEvTablet::TEvGetCountersResponse>();
    }

    std::unique_ptr<TEvTablet::TEvGetCountersResponse> GetCounters()
    {
        SendGetCountersRequest();
        return RecvGetCountersResponse();
    }

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> CreateDescribeBlocksRequest(
        ui32 startIndex,
        ui32 blocksCount,
        const TString& checkpointId = "")
    {
        auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(blocksCount);
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> CreateDescribeBlocksRequest(
        const TBlockRange32& range, const TString& checkpointId = "")
    {
        return CreateDescribeBlocksRequest(range.Start, range.Size(), checkpointId);
    }

    std::unique_ptr<TEvVolume::TEvGetUsedBlocksRequest> CreateGetUsedBlocksRequest()
    {
        return std::make_unique<TEvVolume::TEvGetUsedBlocksRequest>();
    }

    std::unique_ptr<TEvPartitionPrivate::TEvReadBlobRequest> CreateReadBlobRequest(
        const NKikimr::TLogoBlobID& blobId,
        const ui32 bSGroupId,
        const TVector<ui16>& blobOffsets,
        TGuardedSgList sglist)
    {
        auto request =
            std::make_unique<TEvPartitionPrivate::TEvReadBlobRequest>(
                blobId,
                MakeBlobStorageProxyID(bSGroupId),
                blobOffsets,
                std::move(sglist),
                bSGroupId,
                false,          // async
                TInstant::Max() // deadline
            );
        return request;
    }

    std::unique_ptr<TEvVolume::TEvCompactRangeRequest> CreateCompactRangeRequest(
        ui32 blockIndex,
        ui32 blocksCount)
    {
        auto request = std::make_unique<TEvVolume::TEvCompactRangeRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(blocksCount);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest> CreateGetCompactionStatusRequest(
        const TString& operationId)
    {
        auto request = std::make_unique<TEvVolume::TEvGetCompactionStatusRequest>();
        request->Record.SetOperationId(operationId);
        return request;
    }

    std::unique_ptr<TEvPartition::TEvDrainRequest> CreateDrainRequest()
    {
        return std::make_unique<TEvPartition::TEvDrainRequest>();
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        return std::make_unique<NMon::TEvRemoteHttpInfo>(params, method);
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params)
    {
        return std::make_unique<NMon::TEvRemoteHttpInfo>(params);
    }

    void SendRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        auto request = CreateRemoteHttpInfo(params, method);
        SendToPipe(std::move(request));
    }

    void SendRemoteHttpInfo(
        const TString& params)
    {
        auto request = CreateRemoteHttpInfo(params);
        SendToPipe(std::move(request));
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RecvCreateRemoteHttpInfoRes()
    {
        return RecvResponse<NMon::TEvRemoteHttpInfoRes>();
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        auto request = CreateRemoteHttpInfo(params, method);
        SendToPipe(std::move(request));

        auto response = RecvResponse<NMon::TEvRemoteHttpInfoRes>();
        return response;
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params)
    {
        return RemoteHttpInfo(params, HTTP_METHOD::HTTP_METHOD_GET);
    }

    std::unique_ptr<TEvVolume::TEvCheckRangeRequest>
    CreateCheckRangeRequest(TString id, ui32 startIndex, ui32 size, bool calculateChecksums = false)
    {
        auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>();
        request->Record.SetDiskId(id);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(size);
        request->Record.SetCalculateChecksums(calculateChecksums);
        return request;
    }

#define BLOCKSTORE_PARTITION2_COMMON_REQUESTS_PRIVATE(xxx, ...)                \
    xxx(TrimFreshLog,              __VA_ARGS__)                                \
// BLOCKSTORE_PARTITION2_COMMON_REQUESTS_PRIVATE

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        SendToPipe(                                                            \
            Create##name##Request(std::forward<Args>(args)...));               \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        Send##name##Request(std::forward<Args>(args)...);                      \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvPartition)
    BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionPrivate)
    BLOCKSTORE_PARTITION2_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionCommonPrivate)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(BLOCKSTORE_DECLARE_METHOD, TEvVolume)

#undef BLOCKSTORE_DECLARE_METHOD
};

TTestActorRuntime::TEventObserver StorageStateChanger(
    ui32 flag,
    TMaybe<ui32> groupIdFilter = {})
{
    return [=] (TAutoPtr<IEventHandle>& event) {
        switch (event->GetTypeRewrite()) {
            case TEvBlobStorage::EvPutResult: {
                auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                if (!groupIdFilter.Defined() || *groupIdFilter == msg->GroupId) {
                    const_cast<TStorageStatusFlags&>(msg->StatusFlags).Merge(
                        ui32(NKikimrBlobStorage::StatusIsValid) | ui32(flag)
                    );
                    break;
                }
            }
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TEmptyBlock {};

TEmptyBlock Empty()
{
    return {};
}

struct TBlobBlock
{
    TBlobBlock(
        ui32 number,
        ui8 offset,
        ui8 blocksCount = 1,
        ui32 channel = 0,
        ui32 generation = 0
    )
        : Number(number)
        , Offset(offset)
        , BlocksCount(blocksCount)
        , Channel(channel)
        , Generation(generation)
    {}

    ui32 Number;
    ui8 Offset;
    ui8 BlocksCount;
    ui32 Channel;
    ui32 Generation;
};

TBlobBlock Blob(
    ui32 number,
    ui8 offset,
    ui8 blocksCount = 1,
    ui32 channel = 0,
    ui32 generation = 0)
{
    return TBlobBlock(number, offset, blocksCount, channel, generation);
}

struct TFreshBlock
{
    TFreshBlock(ui8 value)
        : Value(value)
    {}

    ui8 Value;
};

TFreshBlock Fresh(ui8 value)
{
    return TFreshBlock(value);
}

using TBlockDescription = std::variant<TEmptyBlock, TBlobBlock, TFreshBlock>;

using TPartitionContent = TVector<TBlockDescription>;

////////////////////////////////////////////////////////////////////////////////

class TTestVolumeProxyActor final
    : public TActorBootstrapped<TTestVolumeProxyActor>
{
private:
    ui64 BaseTabletId;
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    TPartitionContent BasePartitionContent;
    ui32 BaseDiskBlockSize;

public:
    TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 baseDiskBlockSize = DefaultBlockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksRequest(
        const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetChangedBlocksRequest(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TTestVolumeProxyActor::TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 baseDiskBlockSize)
    : BaseTabletId(baseTabletId)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , BasePartitionContent(std::move(basePartitionContent))
    , BaseDiskBlockSize(baseDiskBlockSize)
{}

void TTestVolumeProxyActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Become(&TThis::StateWork);
}

void TTestVolumeProxyActor::HandleDescribeBlocksRequest(
    const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, record.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, record.GetCheckpointId());

    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();

    auto blockIndex = 0;
    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlobBlock>(descr)) {
            const auto& blob = std::get<TBlobBlock>(descr);

            NKikimr::TLogoBlobID blobId(
                BaseTabletId,
                blob.Generation,
                blob.Number,
                blob.Channel,
                BaseDiskBlockSize * 0x100,
                0);
            auto& piece = *response->Record.AddBlobPieces();
            LogoBlobIDFromLogoBlobID(
                blobId,
                piece.MutableBlobId());

            auto& r = *piece.AddRanges();
            r.SetBlobOffset(blob.Offset);
            r.SetBlockIndex(blockIndex);
            r.SetBlocksCount(blob.BlocksCount);

            blockIndex += blob.BlocksCount;
        } else if (std::holds_alternative<TFreshBlock>(descr)) {
            const auto& fresh = std::get<TFreshBlock>(descr);

            auto& r = *response->Record.AddFreshBlockRanges();
            r.SetStartIndex(blockIndex);
            r.SetBlocksCount(1);
            r.SetBlocksContent(TString(BaseDiskBlockSize, char(fresh.Value)));

            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmptyBlock>(descr));
            ++blockIndex;
        }
    }

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleGetChangedBlocksRequest(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, record.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, record.GetHighCheckpointId());
    UNIT_ASSERT_VALUES_EQUAL("", record.GetLowCheckpointId());

    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>();
    ui64 blockIndex = 0;

    TVector<ui8> changedBlocks((record.GetBlocksCount() + 7) / 8);

    auto fillBlock = [&](ui64 block) {
        if (block < record.GetStartIndex() || block >= record.GetStartIndex() + record.GetBlocksCount()) {
            return;
        }

        ui64 bit = block - record.GetStartIndex();
        changedBlocks[bit / 8] |= 1 << (bit % 8);
    };

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlobBlock>(descr)) {
            const auto& blob = std::get<TBlobBlock>(descr);
            for (ui64 block = blockIndex; block < blockIndex + blob.BlocksCount; block++) {
                fillBlock(block);
            }
            blockIndex += blob.BlocksCount;
        } else if (std::holds_alternative<TFreshBlock>(descr)) {
            fillBlock(blockIndex);
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmptyBlock>(descr));
            ++blockIndex;
        }
    }

    for (const auto& b: changedBlocks) {
        response->Record.MutableMask()->push_back(b);
    }

    ctx.Send(ev->Sender, response.release());
}

STFUNC(TTestVolumeProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocksRequest);
        HFunc(TEvService::TEvGetChangedBlocksRequest, HandleGetChangedBlocksRequest);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool EventHandler(
    ui64 tabletId,
    const TEvBlobStorage::TEvGet::TPtr& ev,
    TTestActorRuntimeBase& runtime,
    ui32 blockSize = DefaultBlockSize)
{
    bool result = false;

    auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvBlobStorage::TEvGetResult>(
            NKikimrProto::OK, msg->QuerySize, 0 /* groupId */);

    for (ui32 i = 0; i < msg->QuerySize; ++i) {
        const auto& q = msg->Queries[i];
        const auto& blobId = q.Id;

        UNIT_ASSERT_C(
            !result || blobId.TabletID() == tabletId,
            "All blobs in one TEvGet request should belong to one partition;"
            " tabletId: " << tabletId <<
            " blobId: " << blobId);

        if (blobId.TabletID() == tabletId) {
            result = true;

            auto& r = response->Responses[i];

            r.Id = blobId;
            r.Status = NKikimrProto::OK;

            UNIT_ASSERT(q.Size % blockSize == 0);
            UNIT_ASSERT(q.Shift % blockSize == 0);

            auto blobOffset = q.Shift / blockSize;

            const auto blobEnd = blobOffset + q.Size / blockSize;
            for (; blobOffset < blobEnd; ++blobOffset) {
                UNIT_ASSERT_C(
                    blobOffset <= 0xff,
                    "Blob offset should fit in one byte");

                // Debugging is easier when block content is equal to blob offset.
                r.Buffer.Insert(r.Buffer.End(), TRope(TString(blockSize, char(blobOffset))));
            }
        }
    }

    if (result) {
        runtime.Schedule(
            new IEventHandle(
                ev->Sender,
                ev->Recipient,
                response.release(),
                0,
                ev->Cookie),
            TDuration());
    }
    return result;
}


////////////////////////////////////////////////////////////////////////////////

struct TPartitionWithRuntime
{
    std::unique_ptr<TTestActorRuntime> Runtime;
    std::unique_ptr<TPartitionClient> Partition;
};

////////////////////////////////////////////////////////////////////////////////

TPartitionWithRuntime SetupOverlayPartition(
    ui64 overlayTabletId,
    ui64 baseTabletId,
    const TPartitionContent& basePartitionContent = {},
    TMaybe<ui32> channelsCount = {},
    ui32 blockSize = DefaultBlockSize)
{
    TPartitionWithRuntime result;

    result.Runtime = PrepareTestActorRuntime(
        DefaultConfig(),
        10240,
        channelsCount,
        {
            "overlay-disk",
            "base-disk",
            "checkpoint",
            overlayTabletId,
            baseTabletId,
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
        },
        std::make_unique<TTestVolumeProxyActor>(
            baseTabletId,
            "base-disk",
            "checkpoint",
            basePartitionContent,
            blockSize
        )
    );

    result.Partition = std::make_unique<TPartitionClient>(*result.Runtime);
    result.Partition->WaitReady();

    result.Runtime->SetEventFilter([=] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        bool handled = false;

        const auto wrapped = [&] (const auto& ev) {
            handled = EventHandler(baseTabletId, ev, runtime, blockSize);
        };

        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, wrapped);
        }
        return handled;
    });

    return result;
}

TString GetBlocksContent(
    const TPartitionContent& content,
    size_t blockSize = DefaultBlockSize)
{
    TString result;

    for (const auto& descr: content) {
        if (std::holds_alternative<TEmptyBlock>(descr)) {
            result += TString(blockSize, char(0));
        } else if (std::holds_alternative<TBlobBlock>(descr)) {
            const auto& blob = std::get<TBlobBlock>(descr);

            for (auto i = 0; i < blob.BlocksCount; ++i) {
                const auto blobOffset = blob.Offset + i;
                // Debugging is easier when block content is equal to blob offset.
                result += TString(blockSize, char(blobOffset));
            }
        } else if (std::holds_alternative<TFreshBlock>(descr)) {
            const auto& fresh = std::get<TFreshBlock>(descr);
            result += TString(blockSize, char(fresh.Value));
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    return result;
}

TString BuildRemoteHttpQuery(ui64 tabletId, const TVector<std::pair<TString, TString>>& keyValues)
{
    auto res = TStringBuilder()
        << "/app?TabletID="
        << tabletId;
    for (const auto& p : keyValues) {
        res << "&" << p.first << "=" << p.second;
    }
    return res;
}

template <typename TRequest>
void SendUndeliverableRequest(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& event,
    std::unique_ptr<TRequest> request)
{
    auto fakeRecipient = TActorId(
        event->Recipient.NodeId(),
        event->Recipient.PoolID(),
        0,
        event->Recipient.Hint());
    auto undeliveryActor = event->GetForwardOnNondeliveryRecipient();
    runtime.Send(
        new IEventHandle(
            fakeRecipient,
            event->Sender,
            request.release(),
            event->Flags,
            0,
            &undeliveryActor),
        0);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartition2Test)
{
    Y_UNIT_TEST(ShouldWaitReady)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.StatPartition();
    }

    Y_UNIT_TEST(ShouldDieIfBootingTakesTooMuchTime)
    {
        NProto::TStorageServiceConfig config;
        config.SetPartitionBootTimeout(TDuration::Seconds(10).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(std::move(config));

        bool tabletWasKilled = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() == TEvTablet::EvRestored) {
                    return true;
                } else if (
                    ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType)
                {
                    tabletWasKilled = true;
                }
                return false;
            });

        TPartitionClient partition(*runtime);
        partition.SendWaitReadyRequest();

        runtime->DispatchEvents({}, TDuration::MilliSeconds(1));

        runtime->AdvanceCurrentTime(TDuration::Seconds(15));

        // Wait for partition to die
        auto options = TDispatchOptions();
        options.FinalEvents.emplace_back(TEvents::TEvPoisonPill::EventType);
        runtime->DispatchEvents(options, TDuration::MilliSeconds(100));

        UNIT_ASSERT(tabletWasKilled);
    }

    Y_UNIT_TEST(ShouldRecoverStateOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.RebootTablet();

        partition.StatPartition();
    }

    Y_UNIT_TEST(ShouldStoreBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), GetBlockContent(2));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));
    }

    Y_UNIT_TEST(ShouldStoreBlocksAtTheEndOfAMaxSizeDisk)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange = TBlockRange32::MakeClosedInterval(
            Max<ui32>() - 500,
            Max<ui32>() - 2);
        partition.WriteBlocks(blockRange, 1);

        for (auto blockIndex: xrange(blockRange)) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(blockIndex)),
                GetBlockContent(1)
            );
        }

        partition.WriteBlocks(Max<ui32>() - 2, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2)),
            GetBlockContent(2)
        );

        partition.Flush();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2)),
            GetBlockContent(2)
        );

        partition.Compaction();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2)),
            GetBlockContent(2)
        );
    }

    Y_UNIT_TEST(ShouldProperlySaveBlobUpdatesAfterBatchedWrites)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteRequestBatchingEnabled(true);
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetWriteBlobThreshold(200_KB); // increasing the chance of batching
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 4);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 5);

        // making sparse writes is important for this test
        // we need to get multiple blob updates for the same commit
        for (ui32 i = 0; i < 1000; i += 2) {
            partition.SendWriteBlocksRequest(i, 6);
        }

        for (ui32 i = 0; i < 1000; i += 2) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // checking that batching succeeded at least a couple times
        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        const auto batchCount = stats.GetUserWriteCounters().GetBatchCount();
        UNIT_ASSERT(batchCount < 500);
        UNIT_ASSERT(batchCount > 0);

        // we need the tablet to reinit its state from local db
        partition.RebootTablet();

        for (ui32 i = 1; i < 1000; i += 2) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(5)
            );
        }

        for (ui32 i = 0; i < 1000; i += 2) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(6)
            );
        }

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldBatchSmallWrites)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteRequestBatchingEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool dropProcessWriteQueue = true;
        ui32 cnt = 0;
        bool batchSeen = false;
        NActors::TActorId partActorId;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvProcessWriteQueue: {
                        batchSeen = true;
                        if (dropProcessWriteQueue) {
                            partActorId = event->Sender;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                    case TEvService::EvWriteBlocksRequest: {
                        if (++cnt == 1000 && batchSeen) {
                            dropProcessWriteQueue = false;
                            auto req =
                                std::make_unique<TEvPartitionPrivate::TEvProcessWriteQueue>();
                            runtime->Send(
                                new IEventHandle(
                                    partActorId,
                                    event->Sender,
                                    req.release(),
                                    0, // flags
                                    0),
                                0);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < 1000; ++i) {
            partition.SendWriteBlocksRequest(i, i);
        }

        for (ui32 i = 0; i < 1000; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMergedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(1000, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            stats.GetUserWriteCounters().GetRequestsCount()
        );
        const auto batchCount = stats.GetUserWriteCounters().GetBatchCount();
        UNIT_ASSERT(batchCount < 1000);
        UNIT_ASSERT(batchCount > 0);

        for (ui32 i = 0; i < 1000; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(i)
            );
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldRespectMaxBlobRangeSizeDuringBatching)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        const auto maxBlobRangeSize = 2048;
        config.SetMaxBlobRangeSize(maxBlobRangeSize * 4_KB);
        auto runtime = PrepareTestActorRuntime(config, maxBlobRangeSize * 2);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            partition.SendWriteBlocksRequest(i % 2 ? i : maxBlobRangeSize + i, i);
        }

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                            const auto& blobs = msg->NewBlobs;
                            for (const auto& blob: blobs) {
                                const auto blobRangeSize =
                                    blob.Blocks.back().BlockIndex
                                    - blob.Blocks.front().BlockIndex;
                                Cdbg << blobRangeSize << Endl;
                                UNIT_ASSERT(blobRangeSize <= maxBlobRangeSize);
                            }
                        }

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMergedBlobsCount());

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            const auto block =
                partition.ReadBlocks(i % 2 ? i : maxBlobRangeSize + i);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(block), GetBlockContent(i));
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldCorrectlyOverwriteMergedBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 512), 1);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 512), 2);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 512), 3);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 512), 4);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(111)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(512)), GetBlockContent(4));

        partition.ZeroBlocks(TBlockRange32::WithLength(100, 101));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(111)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(512)), GetBlockContent(4));
    }

    Y_UNIT_TEST(ShouldStoreBlocksUsingLocalAPI)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto blockContent1 = GetBlockContent(1);
        partition.WriteBlocksLocal(1, blockContent1);
        auto blockContent2 = GetBlockContent(2);
        partition.WriteBlocksLocal(2, blockContent2);
        auto blockContent3 = GetBlockContent(3);
        partition.WriteBlocksLocal(3, blockContent3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(1, block);
            UNIT_ASSERT_VALUES_EQUAL(block, GetBlockContent(1));
        }

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(2, block);
            UNIT_ASSERT_VALUES_EQUAL(block, GetBlockContent(2));
        }

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(3, block);
            UNIT_ASSERT_VALUES_EQUAL(block, GetBlockContent(3));
        }
    }

    Y_UNIT_TEST(ShouldRecoverBlocksOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(2, 3);
        partition.WriteBlocks(2, 4);
        partition.WriteBlocks(3, 5);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 5);

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(5));
    }

    Y_UNIT_TEST(ShouldMergeVersionsOnRead)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        // partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(11));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), GetBlockContent(22));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(33));
    }

    Y_UNIT_TEST(ShouldZeroBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        partition.Flush();
        partition.ZeroBlocks(2);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));

        // partition.Flush();
        partition.ZeroBlocks(3);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), "");
    }

    Y_UNIT_TEST(ShouldReadZeroFromUninitializedBlock)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        // partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));
    }

    Y_UNIT_TEST(ShouldZeroLargeNumberOfBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(0)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(MaxBlocksCount / 2)), "");
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(MaxBlocksCount - 1)), "");
    }

    Y_UNIT_TEST(ShouldFlushAsNewBlob)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 0);
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), GetBlockContent(2));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlush)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), MaxBlocksCount - 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 0);
        }

        partition.WriteBlocks(MaxBlocksCount - 1);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), MaxBlocksCount);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
        }
    }

    Y_UNIT_TEST(ShouldAddSmallNonDeletionBlobsDuringFlush)
    {
        auto config = DefaultConfig(4_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), MaxBlocksCount - 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 0);
        }

        static constexpr ui32 extraBlocks = 4;
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount - 1, extraBlocks + 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), MaxBlocksCount + extraBlocks);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }
    }

    Y_UNIT_TEST(ShouldReplaceBlobsOnCompaction)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetHDDV2MaxBlobsPerRange(compactionThreshold);
        config.SetSSDV2MaxBlobsPerRange(compactionThreshold);
        config.SetCompactionGarbageThreshold(100);  // disabling garbage-based compaction

        auto runtime = PrepareTestActorRuntime(config, 1024 * (compactionThreshold + 1));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.WriteBlocks((i + 1) * 1024, (i + 1) * 1024);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), compactionThreshold - 1);
        }

        partition.Compaction(0);    // force compaction

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), compactionThreshold);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2)), GetBlockContent(2));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(3)), GetBlockContent(3));
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompaction)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetHDDV2MaxBlobsPerRange(compactionThreshold);
        config.SetSSDV2MaxBlobsPerRange(compactionThreshold);
        config.SetCompactionGarbageThreshold(100);  // disabling garbage-based compaction

        auto runtime = PrepareTestActorRuntime(config, 1024 * (compactionThreshold + 1));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.WriteBlocks((i + 1) * 1024, (i + 1) * 1024);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), compactionThreshold - 1);
        }

        partition.WriteBlocks(0, 0);
        partition.WriteBlocks(1024, 1024);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), compactionThreshold + 1);
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunLoadOptimizingCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetHDDV2MaxBlobsPerRange(999);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 0; i < 512; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 2, 2));
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 512);
        }

        bool compactionRequestObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::RangeCompaction) {
                            compactionRequestObserved = true;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 10; ++j) {
                partition.ReadBlocks(TBlockRange32::WithLength(j, 101));
            }
        }

        // triggering EnqueueCompactionIfNeeded(...)
        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequestObserved);
    }

    Y_UNIT_TEST(RangeCompactionShouldRunIncrementally)
    {
        auto config = DefaultConfig();
        config.SetMaxSkippedBlobsDuringCompaction(3);
        config.SetTargetCompactionBytesPerOp(512_KB);
        // disabling automatic compaction and cleanup
        config.SetHDDCompactionType(NProto::CT_DEFAULT);
        config.SetHDDV2MaxBlobsPerRange(999);
        config.SetSSDV2MaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetUpdateBlobsThreshold(999999);
        // disabling fresh blocks
        config.SetWriteBlobThreshold(1);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::WithLength(0, 1024));   // blob1: 1024 blocks
        partition.WriteBlocks(
            TBlockRange32::WithLength(0, 301));   // blob2: 301 block
        partition.WriteBlocks(
            TBlockRange32::WithLength(400, 101));   // blob3: 101 block
        partition.WriteBlocks(
            TBlockRange32::WithLength(501, 100));   // blob4: 100 blocks
        partition.WriteBlocks(
            TBlockRange32::WithLength(601, 100));   // blob5: 100 blocks
        partition.WriteBlocks(
            TBlockRange32::WithLength(701, 100));   // blob6: 100 blocks

        partition.Compaction(0);
        partition.Cleanup();

        {
            // blob4, blob5, blob6 compacted into blob7: 300 blocks
            // blob4, blob5, blob6 got deleted

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                stats.GetUsedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1024 + 301 + 101 + 300,
                stats.GetMergedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                stats.GetMergedBlobsCount()
            );
        }

        partition.WriteBlocks(
            TBlockRange32::WithLength(801, 5));   // blob8: 5 blocks
        partition.WriteBlocks(
            TBlockRange32::WithLength(806, 5));   // blob9: 5 blocks
        partition.WriteBlocks(
            TBlockRange32::WithLength(811, 5));   // blob10: 5 blocks

        partition.Compaction(0);
        partition.Cleanup();

        {
            // blob8, blob9, blob10, blob3, blob7 compacted into blob11: 416 blocks
            // blob8, blob9, blob10, blob3 would not be enough because their
            // total byte size is less than 512_KB

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                stats.GetUsedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1024 + 301 + 101 + 300 + 15,
                stats.GetMergedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                stats.GetMergedBlobsCount()
            );
        }

        partition.Compaction(0, true);
        partition.Cleanup();

        {
            // blob1, blob2, blob11 compacted into several blobs:
            //

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                stats.GetUsedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                stats.GetMergedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.GetMergedBlobsCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldGetUsedBlocks)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto response = partition.GetUsedBlocks();
        // TODO(NBS-2364): actually, GetUsedBlocks doesn't supported in partition2
        // but requests should return OK
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldValidateMaxChangedBlocksRangeBlocksCount)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 1 << 21);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.SendGetChangedBlocksRequest(
                TBlockRange32::WithLength(0, 1 << 21),
                "",
                "");

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                partition.RecvGetChangedBlocksResponse()->GetError().GetCode());
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1 << 20),
                "",
                "");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldSeeChangedFreshBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(2, 2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 1024),
            "cp1",
            "cp2");
        UNIT_ASSERT_C(response->Record.GetMask().size() == 128, "mask size mismatch");
        UNIT_ASSERT_C(response->Record.GetMask()[0] == 6, "mask mismatch");
    }

    Y_UNIT_TEST(ShouldSeeChangedMergedBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024),1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024),1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024),2);
        partition.WriteBlocks(TBlockRange32::WithLength(2048,1024),2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 3072),
            "cp1",
            "cp2");

        const auto& mask = response->Record.GetMask();
        UNIT_ASSERT_C(mask.size() == 384, "mask size mismatch");
        AssertEqual(
            TVector<ui8>(mask.begin() + 128, mask.end()),
            TVector<ui8>(256, 255));
    }

    Y_UNIT_TEST(ShouldUseZeroCommitIdWhenLowCheckpointIdIsNotSet)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.CreateCheckpoint("cp1");
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "cp2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(mask.size(), 384);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], 2);
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "",
                "cp2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(mask.size(), 384);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], 3);
        }
    }

    Y_UNIT_TEST(ShouldUseMostRecentCommitIdWhenHighCheckpointIdIsNotSet)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 1);

        partition.CreateCheckpoint("cp2");

        partition.WriteBlocks(2, 1);

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "cp2");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(mask.size(), 384);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], 2);
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "");

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(mask.size(), 384);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], 6);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleStartBlockInChangedBlocksRequest)
    {
        auto config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1024, 1);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "cp1",
                "cp2");

            const auto &mask = response->Record.GetMask();
            UNIT_ASSERT_C(mask.size() == 128, "mask size mismatch");
            UNIT_ASSERT_C(((ui8) mask[0]) == 1, "mask mismatch");
            AssertEqual(
                TVector<ui8>(mask.begin() + 1, mask.end()),
                TVector<ui8>(127, 0));
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(1024, 1024),
                "cp1",
                "cp2");

            const auto &mask = response->Record.GetMask();
            UNIT_ASSERT_C(mask.size() == 128, "mask size mismatch");
            UNIT_ASSERT_C(((ui8) mask[0]) == 1, "mask mismatch");
            AssertEqual(
                TVector<ui8>(mask.begin() + 1, mask.end()),
                TVector<ui8>(127, 0));
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 2048),
                "cp1",
                "cp2");

            const auto &mask = response->Record.GetMask();
            UNIT_ASSERT_C(mask.size() == 256, "mask size mismatch");

            UNIT_ASSERT_C(((ui8) mask[0]) == 1, "mask mismatch");
            AssertEqual(
                TVector<ui8>(mask.begin() + 1, mask.begin() + 128),
                TVector<ui8>(127, 0));

            UNIT_ASSERT_C(((ui8) mask[128]) == 1, "mask mismatch");
            AssertEqual(
                TVector<ui8>(mask.begin() + 129, mask.end()),
                TVector<ui8>(127, 0));
        }
    }

    Y_UNIT_TEST(ShouldReadFromMixedAndRangeZonesIntersection)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetHotZoneRequestCountFactor(2);
        config.SetColdZoneRequestCountFactor(1);
        auto runtime = PrepareTestActorRuntime(
            config,
            2048,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            1024
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // write some blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }
        // write to mixed&ranges intersection
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 4);
        partition.WriteBlocks(TBlockRange32::WithLength(1000, 501), 5);
        // apply updates
        partition.Cleanup();
        // compact
        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(3));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(999)), GetBlockContent(3));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1000)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1500)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1501)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2047)), GetBlockContent(4));
    }

    Y_UNIT_TEST(ShouldReadFromMixedAndRangeZonesIntersection2)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetHotZoneRequestCountFactor(1);
        config.SetColdZoneRequestCountFactor(1);
        auto runtime = PrepareTestActorRuntime(
            config,
            2048,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            1024
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // write some blocks to pure range zones
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 4);
        partition.WriteBlocks(TBlockRange32::WithLength(1000, 501), 5);
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }
        // apply updates
        partition.Cleanup();
        // compact
        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(3));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(999)), GetBlockContent(3));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1000)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1500)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1501)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(2047)), GetBlockContent(4));
    }

    Y_UNIT_TEST(ShouldReadFromCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("checkpoint1");

        partition.Flush();
        partition.WriteBlocks(1, 2);
        partition.CreateCheckpoint("checkpoint2");

        partition.Flush();
        partition.WriteBlocks(1, 3);
        partition.CreateCheckpoint("checkpoint3");

        partition.WriteBlocks(1, 4);

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1, "checkpoint1")), GetBlockContent(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1, "checkpoint2")), GetBlockContent(2));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1, "checkpoint3")), GetBlockContent(3));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1)), GetBlockContent(4));
    }

    Y_UNIT_TEST(ShouldReadFromCheckpoint2)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 4);
        partition.CreateCheckpoint("checkpoint1");

        partition.WriteBlocks(TBlockRange32::WithLength(0, 513), 5);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(523, 1023), 6);
        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(512, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(513, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(523, "checkpoint1")), GetBlockContent(4));
    }

    Y_UNIT_TEST(ShouldReadFromCheckpointWithMixedIndex)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetHotZoneRequestCountFactor(1);
        config.SetColdZoneRequestCountFactor(1);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 4);
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
        partition.CreateCheckpoint("checkpoint1");

        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 513), 5);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(523, 1023), 6);
        partition.Cleanup();
        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(0, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(511, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(512, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(523, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1023, "checkpoint1")), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(0)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(512)), GetBlockContent(5));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(513)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(522)), GetBlockContent(4));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(523)), GetBlockContent(6));
        UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(partition.ReadBlocks(1023)), GetBlockContent(6));
    }

    Y_UNIT_TEST(ShouldReadFlushedDataFromCheckpointWithMixedIndex)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetHotZoneRequestCountFactor(1);
        config.SetColdZoneRequestCountFactor(1);
        config.SetFlushThreshold(100_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.WriteBlocks(3, 4);
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
        partition.CreateCheckpoint("checkpoint1");

        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1, "checkpoint1")),
            GetBlockContent(1)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(2, "checkpoint1")),
            GetBlockContent(2)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(3, "checkpoint1")),
            GetBlockContent(4)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1)),
            GetBlockContent(1)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(2)),
            GetBlockContent(2)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(3)),
            GetBlockContent(4)
        );
    }

    auto BuildEvGetBreaker(ui32 blockCount, bool& broken) {
        return [blockCount, &broken] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    ui32 totalSize = 0;
                    for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                        totalSize += msg->Responses[i].Buffer.size();
                    }
                    if (totalSize == blockCount * DefaultBlockSize) {
                        // it's our blob
                        msg->Responses[0].Status = NKikimrProto::NODATA;
                        broken = true;
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
    }

    Y_UNIT_TEST(ShouldReturnErrorIfNODATABlocksAreDetectedInDefaultStorageAccessMode)
    {
        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            {},
            {},
            EStorageAccessMode::Default
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool broken = false;
        runtime->SetObserverFunc(BuildEvGetBreaker(777, broken));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 777), 1);
        partition.SendReadBlocksRequest(TBlockRange32::WithLength(0, 777));
        auto response = partition.RecvReadBlocksResponse();
        Y_ABORT_UNLESS(broken);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldMarkNODATABlocksInRepairStorageAccessMode)
    {
        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            {},
            {},
            EStorageAccessMode::Repair
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool broken = false;
        runtime->SetObserverFunc(BuildEvGetBreaker(777, broken));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 777), 1);
        partition.SendReadBlocksRequest(TBlockRange32::WithLength(0, 777));
        auto response = partition.RecvReadBlocksResponse();
        Y_ABORT_UNLESS(broken);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());

        const auto& blocks = response->Record.GetBlocks();
        UNIT_ASSERT_EQUAL(777, blocks.BuffersSize());
        for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBrokenDataMarker(),
                blocks.GetBuffers(i)
            );
        }
    }

    Y_UNIT_TEST(ShouldNotOverwriteNewerDataWithInflightFlushedBlob)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetFlushThreshold(100_MB);
        config.SetUpdateBlobsThreshold(999999);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(1000, 1);

        TAutoPtr<IEventHandle> addFlushedBlob;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        using TType = TEvPartitionPrivate::TEvAddBlobsRequest;
                        auto* msg = event->Get<TType>();
                        if (msg->NewBlobs[0].Blocks.size() == 2) {
                            addFlushedBlob = event.Release();

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendFlushRequest();

        // double write is needed to force cleanup (we need overwritten blobs)
        partition.WriteBlocks(TBlockRange32::WithLength(0, 501), 2) ;
        partition.WriteBlocks(TBlockRange32::WithLength(0, 501), 2);
        {
            auto resp = partition.Cleanup();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, resp->GetStatus());
        }

        UNIT_ASSERT(addFlushedBlob);
        runtime->Send(addFlushedBlob.Release());
        {
            auto resp = partition.RecvFlushResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, resp->GetStatus());
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1)),
            GetBlockContent(2)
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1000)),
            GetBlockContent(1)
        );
    }

    Y_UNIT_TEST(ShouldReadFromCheckpointRightBeforeReboot)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);

        partition.CreateCheckpoint("checkpoint1");

        partition.RebootTablet();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.Compaction();

        for (ui32 i = 0; i < 1024; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i, "checkpoint1")),
                GetBlockContent(2)
            );
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 4);
        partition.Compaction();

        for (ui32 i = 0; i < 1024; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i, "checkpoint1")),
                GetBlockContent(2)
            );
        }
    }

    Y_UNIT_TEST(ShouldNotReadBlocksZeroedInMixedIndexAfterCheckpoint)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetSSDV2MaxBlobsPerRange(999999);
        config.SetHDDV2MaxBlobsPerRange(999999);
        config.SetUpdateBlobsThreshold(999999);
        config.SetCompactionGarbageThreshold(999999);
        config.SetHotZoneRequestCountFactor(1);
        config.SetColdZoneRequestCountFactor(1);
        config.SetFlushThreshold(100_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 301), 1);
        // overwriting our data once - otherwise cleanup will have no effect
        partition.WriteBlocks(TBlockRange32::WithLength(0, 301), 2);
        partition.CreateCheckpoint("checkpoint1");

        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }

        // this cleanup will just flush dirty blobs
        partition.Cleanup();

        // writing and overwriting some blocks to cause cleanup
        partition.WriteBlocks(TBlockRange32::WithLength(700, 301), 3);
        partition.WriteBlocks(TBlockRange32::WithLength(700, 301), 4);

        partition.ZeroBlocks(0);
        //partition.WriteBlocks(1, GetBlockContent(5));
        //partition.Flush();
        partition.Cleanup();
        partition.RebootTablet();

        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(0, response->MixedZoneCount);
        }

        for (ui32 i = 0; i <= 300; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i, "checkpoint1")),
                GetBlockContent(2)
            );
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(0)),
            TString()
        );

        /*
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1)),
            GetBlockContent(3)
        );
        */

        for (ui32 i = 2; i <= 300; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(2)
            );
        }
    }

    Y_UNIT_TEST(ShouldSupportCheckpointOperations)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.CreateCheckpoint("checkpoint1");
        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpoint("checkpoint1");
        partition.DeleteCheckpoint("checkpoint1");
    }

    Y_UNIT_TEST(ShouldDeleteCheckpointAfterDeleteCheckpointData)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.CreateCheckpoint("checkpoint1");
        partition.SendDeleteCheckpointDataRequest("checkpoint1");
        auto resp = partition.RecvDeleteCheckpointDataResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, resp->GetStatus());
    }

    Y_UNIT_TEST(CheckpointDeletionShouldTriggerCleanup)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.CreateCheckpoint("checkpoint1");

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        partition.Cleanup();

        int cleanupMode = -1;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCleanupRequest: {
                        using TType = TEvPartitionPrivate::TEvCleanupRequest;
                        auto* msg = event->Get<TType>();
                        cleanupMode = msg->Mode;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // marking checkpoint1 as deleted - this should trigger a cleanup request
        partition.DeleteCheckpoint("checkpoint1");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(TEvPartitionPrivate::CheckpointBlobCleanup),
            cleanupMode
        );
    }

    Y_UNIT_TEST(ShouldDeleteCheckpointBlobs)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetUpdateBlobsThreshold(999999);
        config.SetSSDV2MaxBlobsPerRange(999999);
        config.SetHDDV2MaxBlobsPerRange(999999);
        config.SetCompactionGarbageThreshold(999999);
        config.SetFlushThreshold(1_GB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 1000 distinct blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1000), 1);
        // 2 distinct blocks
        partition.WriteBlocks(1000, 10);
        partition.WriteBlocks(1000, 11);
        partition.WriteBlocks(1001, 12);
        partition.Flush();
        // 3 more distinct blocks
        partition.WriteBlocks(1002, 13);
        partition.WriteBlocks(1002, 14);
        partition.WriteBlocks(1003, 15);
        partition.WriteBlocks(1004, 16);
        partition.CreateCheckpoint("checkpoint1");
        partition.Flush();

        // 1000 distinct blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1000), 2);
        partition.CreateCheckpoint("checkpoint2");

        // 1005 distinct blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1005), 3);

        // causing block rebasing => CheckpointBlobs table will be filled
        // 2 checkpoints => 3 chunks in DeletedRangeMap => 3 cleanups needed
        partition.Cleanup();
        partition.Cleanup();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 2005);
        }

        // marking checkpoint1 as deleted - this should trigger a cleanup request
        partition.DeleteCheckpoint("checkpoint1");
        partition.Cleanup(TEvPartitionPrivate::CheckpointBlobCleanup);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 1005);
        }

        for (ui32 i = 0; i < 1000; ++i) {
            partition.SendReadBlocksRequest(i, "checkpoint1");
            auto resp = partition.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, resp->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i, "checkpoint2")),
                GetBlockContent(2)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(3)
            );
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1000, "checkpoint2")),
            GetBlockContent(11)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1001, "checkpoint2")),
            GetBlockContent(12)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1002, "checkpoint2")),
            GetBlockContent(14)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1003, "checkpoint2")),
            GetBlockContent(15)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(partition.ReadBlocks(1004, "checkpoint2")),
            GetBlockContent(16)
        );
        for (ui32 i = 1000; i <= 1004; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(i)),
                GetBlockContent(3)
            );
        }

        partition.DeleteCheckpoint("checkpoint2");
        partition.Cleanup(TEvPartitionPrivate::CheckpointBlobCleanup);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
        }
    }

    Y_UNIT_TEST(ShouldRejectRequestsInProgressOnTabletDeath)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvWriteBlobResponse) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // kill tablet
        partition.RebootTablet();

        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(response->GetStatus() == E_REJECTED, response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectWriteRequestsIfDataChannelsAreYellow)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(StorageStateChanger(
            NKikimrBlobStorage::StatusDiskSpaceYellowStop));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyTrackYellowChannelsInBackground)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        // one data channel yellow => ok
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>({
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // all channels yellow => failure
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({
                        env.GetGroupIds()[0],
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>({
                        env.GetGroupIds()[0],
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        // channels returned to non-yellow state => ok
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>(),
                    TVector<ui32>(),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // TODO: the state may be neither yellow nor green (e.g. orange) -
        // this case is not supported in the background check, but should be
    }

    Y_UNIT_TEST(ShouldRejectCompactionRequestsIfDataChannelsAreAlmostFull)
    {
        // smoke test: tests that the compaction mechanism checks partition state
        // detailed tests are located in part2_state_ut.cpp
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(
            StorageStateChanger(NKikimrBlobStorage::StatusDiskSpaceYellowStop));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        {
            auto response = partition.Compaction();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());
        }

        const auto badFlags = {
            NKikimrBlobStorage::StatusDiskSpaceOrange,
            NKikimrBlobStorage::StatusDiskSpaceRed,
        };

        for (const auto flag: badFlags) {
            partition.RebootTablet();

            runtime->SetObserverFunc(StorageStateChanger(flag));
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

            auto request = partition.CreateCompactionRequest();
            partition.SendToPipe(std::move(request));

            auto response = partition.RecvResponse<TEvPartitionPrivate::TEvCompactionResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_OUT_OF_SPACE, response->GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldReassignNonwritableTabletChannels)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;
        const auto reassignTimeout = TDuration::Minutes(1);

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();

        NMonitoring::TDynamicCountersPtr counters
            = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto reassignCounter =
            counters->GetCounter("AppCriticalEvents/ReassignTablet", true);

        NProto::TStorageServiceConfig config;
        config.SetReassignRequestRetryTimeout(reassignTimeout.MilliSeconds());
        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            channelCount,
            config);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;
        ui32 ssflags =
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
            NKikimrBlobStorage::StatusDiskSpaceYellowStop;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;

                        break;
                    }
                }

                return StorageStateChanger(ssflags, env.GetGroupIds()[1])(event);
            }
        );

        // first request does not trigger a reassign event - data is written to
        // the first group
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        // second request succeeds but triggers a reassign event - yellow flag
        // is received in the response for this request
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(4, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        {
            // third request doesn't trigger a reassign event because we can
            // still write (some data channels are not yellow yet)
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode()
            );
        }

        // checking that a reassign request has not been sent
        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        reassignedTabletId = 0;
        channels.clear();

        {
            // informing partition tablet that the first group is yellow
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({env.GetGroupIds()[0], env.GetGroupIds()[1]}),
                    TVector<ui32>({env.GetGroupIds()[0], env.GetGroupIds()[1]}),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // this time no reassign request should have been sent because of the
        // ReassignRequestSentTs check in partition actor
        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        runtime.AdvanceCurrentTime(reassignTimeout);

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // ReassignRequestRetryTimeout has passed - another reassign request
        // should've been sent
        UNIT_ASSERT_VALUES_EQUAL(2, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(5, channels.size());
        for (ui32 i = 0; i < channels.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, channels[i]);
        }

        partition.RebootTablet();
        ssflags = {};

        // no yellow channels => write request should succeed
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        partition.RebootTablet();

        {
            // informing partition tablet that the first group is yellow
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({env.GetGroupIds()[0]}),
                    TVector<ui32>({env.GetGroupIds()[0]}),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({});

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // checking that a reassign request has been sent
        UNIT_ASSERT_VALUES_EQUAL(3, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(4, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(0, channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, channels[1]);
        UNIT_ASSERT_VALUES_EQUAL(2, channels[2]);
        UNIT_ASSERT_VALUES_EQUAL(3, channels[3]);
    }

    Y_UNIT_TEST(ShouldReassignNonwritableTabletChannelsAgainAfterErrorFromHiveProxy)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;
        ui32 ssflags =
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
            NKikimrBlobStorage::StatusDiskSpaceYellowStop;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;
                        auto response =
                            std::make_unique<TEvHiveProxy::TEvReassignTabletResponse>(
                                MakeError(E_REJECTED, "error")
                            );
                        runtime->Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            0
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return StorageStateChanger(ssflags)(event);
            }
        );

        // first request is successful since we don't know that our channels
        // are yellow yet
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // but upon EvPutResult we should find out that our channels are yellow
        // and should send a reassign request
        UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        for (ui32 i = 0; i < 3; ++i) {
            {
                // other requests should fail
                auto request = partition.CreateWriteBlocksRequest(
                    TBlockRange32::WithLength(0, 1024));
                partition.SendToPipe(std::move(request));

                auto response =
                    partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
                UNIT_ASSERT_VALUES_EQUAL(
                    E_BS_OUT_OF_SPACE,
                    response->GetError().GetCode()
                );
            }

            // checking that a reassign request has been sent
            UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
            UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
            UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

            reassignedTabletId = 0;
            channels.clear();
        }
    }

    Y_UNIT_TEST(ShouldSendBackpressureReportsUponChannelColorChange)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TBackpressureReport report;

        runtime->SetObserverFunc(
            [&report] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvBackpressureReport: {
                        report = *event->Get<TEvPartition::TEvBackpressureReport>();
                        break;
                    }
                }

                return StorageStateChanger(
                    NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
                    NKikimrBlobStorage::StatusDiskSpaceYellowStop)(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT(report.DiskSpaceScore > 0);
    }

    Y_UNIT_TEST(ShouldSendBackpressureReportsRegularly)
    {
        const auto blockCount = 1000;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TBackpressureReport report;
        bool reportUpdated = false;

        runtime->SetObserverFunc(
            [&report, &reportUpdated] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvBackpressureReport: {
                        report = *event->Get<TEvPartition::TEvBackpressureReport>();
                        reportUpdated = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (int i = 0; i < 200; ++i) {
            partition.WriteBlocks(i, i);
        }

        // Manually sending TEvSendBackpressureReport here
        // In real scenarios TPartitionActor will schedule this event
        // upon executor activation
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvSendBackpressureReport>()
        );

        runtime->DispatchEvents({}, TDuration::Seconds(5));

        UNIT_ASSERT_DOUBLES_EQUAL(3.5, report.FreshIndexScore, 1e-5);
    }

    Y_UNIT_TEST(ShouldReportNumberOfNonEmptyRangesInVolumeStats)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096 * MaxBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 g = 0; g < 4; ++g) {
            partition.WriteBlocks(g * MaxBlocksCount * MaxBlocksCount, 1);
        }

        partition.Flush();

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetNonEmptyRangeCount(), 4);
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithInvalidCheckpointId)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const TString validCheckpoint = "0";
        partition.CreateCheckpoint(validCheckpoint);

        const auto range = TBlockRange32::WithLength(0, 1);
        partition.DescribeBlocks(range, validCheckpoint);

        {
            const TString invalidCheckpoint = "1";
            auto request =
                partition.CreateDescribeBlocksRequest(range, invalidCheckpoint);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldForbidDescribeBlocksWithEmptyRange)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            auto request = partition.CreateDescribeBlocksRequest(0, 0);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(DescribeBlocksIsNotImplementedForOverlayDisks)
    {
        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2);

        auto& partition = *partitionWithRuntime.Partition;
        partition.WaitReady();

        {
            auto request = partition.CreateDescribeBlocksRequest(0, 0);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == E_NOT_IMPLEMENTED);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithOutOfBoundsRange)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 11), char(1));

        {
            const auto range = TBlockRange32::MakeClosedInterval(
                Max<ui32>() - 2,
                Max<ui32>() - 1);
            auto request = partition.CreateDescribeBlocksRequest(range);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == S_OK);
            UNIT_ASSERT(response->Record.FreshBlockRangesSize() == 0);
            UNIT_ASSERT(response->Record.BlobPiecesSize() == 0);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWhenBlocksAreFresh)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(1, 11);
        partition.WriteBlocks(range, char(1));

        const TString checkpoint = "0";
        partition.CreateCheckpoint(checkpoint);

        {
            auto response = partition.DescribeBlocks(range, checkpoint);
            TString actualContent;
            TVector<TBlockRange32> actualRanges;

            auto extractContent = [&]() {
                const auto& message = response->Record;
                for (size_t i = 0; i < message.FreshBlockRangesSize(); ++i) {
                    const auto& freshRange = message.GetFreshBlockRanges(i);
                    actualContent += freshRange.GetBlocksContent();
                    actualRanges.push_back(
                        TBlockRange32::WithLength(
                            freshRange.GetStartIndex(),
                            freshRange.GetBlocksCount()));
                 }
            };

            extractContent();
            TString expectedContent = GetBlocksContent(char(1), range.Size());
            UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);
            CheckRangesArePartition(actualRanges, range);

            response = partition.DescribeBlocks(0, Max<ui32>(), checkpoint);
            actualContent = {};
            actualRanges = {};

            extractContent();
            expectedContent = GetBlocksContent(char(1), range.Size());
            UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);
            CheckRangesArePartition(actualRanges, range);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithOneBlob)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range1 = TBlockRange32::WithLength(11, 11);
        partition.WriteBlocks(range1, char(1));
        const auto range2 = TBlockRange32::WithLength(33, 22);
        partition.WriteBlocks(range2, char(1));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 101));
            const auto& message = response->Record;
            // Expect that all data is contained in one blob.
            UNIT_ASSERT_VALUES_EQUAL(1, message.BlobPiecesSize());

            const auto& blobPiece = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece.RangesSize());
            const auto& r1 = blobPiece.GetRanges(0);
            const auto& r2 = blobPiece.GetRanges(1);

            UNIT_ASSERT_VALUES_EQUAL(0, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range1.Start, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r1.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r2.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range2.Start, r2.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range2.Size(), r2.GetBlocksCount());

            TVector<ui16> blobOffsets;
            for (size_t i = 0; i < r1.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r1.GetBlobOffset() + i);
            }
            for (size_t i = 0; i < r2.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r2.GetBlobOffset() + i);
            }

            {
                const auto blobId =
                    LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
                const auto group = blobPiece.GetBSGroupId();

                TBlockBuffer blobContent;
                for (size_t i = 0; i < blobOffsets.size(); ++i) {
                    blobContent.AddBlock(DefaultBlockSize, 0);
                }

                const auto response = partition.ReadBlob(
                    blobId,
                    group,
                    blobOffsets,
                    TGuardedSgList(blobContent.GetBlocks()));

                for (size_t i = 0; i < blobContent.GetBlocksCount(); ++i) {
                    const auto block = blobContent.GetBlock(i).AsStringBuf();
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        GetBlockContent(char(1)), block, "during iteration #" << i);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldRespondToDescribeBlocksRequestWithDenseBlobs)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.WriteBlocks(1, char(1));
        partition.WriteBlocks(2, char(1));
        partition.Flush();
        partition.WriteBlocks(3, char(2));
        partition.WriteBlocks(4, char(2));
        partition.WriteBlocks(5, char(2));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(1, 4));
            const auto& message = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, message.BlobPiecesSize());

            const auto& blobPiece1 = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece1.RangesSize());
            const auto& r1_2 = blobPiece1.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(1, r1_2.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(1, r1_2.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(2, r1_2.GetBlocksCount());
            const auto blobId1 =
                LogoBlobIDFromLogoBlobID(blobPiece1.GetBlobId());
            const auto group1 = blobPiece1.GetBSGroupId();

            const auto& blobPiece2 = message.GetBlobPieces(1);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece2.RangesSize());
            const auto& r3_5 = blobPiece2.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r3_5.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(3, r3_5.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(2, r3_5.GetBlocksCount());
            const auto blobId2 =
                LogoBlobIDFromLogoBlobID(blobPiece2.GetBlobId());
            const auto group2 = blobPiece2.GetBSGroupId();

            for (ui16 i = 0; i < 2; ++i) {
                const TVector<ui16> offsets = {i};
                {
                    TBlockBuffer blobContent;
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        blobContent.AddBlock(DefaultBlockSize, 0);
                    }

                    const auto response = partition.ReadBlob(
                        blobId1,
                        group1,
                        offsets,
                        TGuardedSgList(blobContent.GetBlocks()));
                    UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(1)), blobContent.GetBlock(0).AsStringBuf());
                }

                {
                    TBlockBuffer blobContent;
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        blobContent.AddBlock(DefaultBlockSize, 0);
                    }

                    const auto response = partition.ReadBlob(
                        blobId2,
                        group2,
                        offsets,
                        TGuardedSgList(blobContent.GetBlocks()));
                    UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(2)), blobContent.GetBlock(0).AsStringBuf());
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldRespondToDescribeBlocksRequestWithSparseBlobs)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.WriteBlocks(2, char(1));
        partition.Flush();
        partition.WriteBlocks(4, char(2));
        partition.WriteBlocks(6, char(2));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 7));
            const auto& message = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, message.BlobPiecesSize());

            const auto& blobPiece1 = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece1.RangesSize());
            const auto& r0 = blobPiece1.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r0.GetBlocksCount());
            const auto& r1 = blobPiece1.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(2, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlocksCount());
            const auto blobId1 =
                LogoBlobIDFromLogoBlobID(blobPiece1.GetBlobId());
            const auto group1 = blobPiece1.GetBSGroupId();

            const auto& blobPiece2 = message.GetBlobPieces(1);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece2.RangesSize());
            const auto& r4 = blobPiece2.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r4.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(4, r4.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r4.GetBlocksCount());
            const auto& r6 = blobPiece2.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(6, r6.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlocksCount());
            const auto blobId2 =
                LogoBlobIDFromLogoBlobID(blobPiece2.GetBlobId());
            const auto group2 = blobPiece2.GetBSGroupId();

            for (ui16 i = 0; i < 1; ++i) {
                const TVector<ui16> offsets = {i};
                {
                    TBlockBuffer blobContent;
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        blobContent.AddBlock(DefaultBlockSize, 0);
                    }

                    const auto response = partition.ReadBlob(
                        blobId1,
                        group1,
                        offsets,
                        TGuardedSgList(blobContent.GetBlocks()));
                    UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(1)), blobContent.GetBlock(0).AsStringBuf());
                }

                {
                    TBlockBuffer blobContent;
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        blobContent.AddBlock(DefaultBlockSize, 0);
                    }

                    const auto response = partition.ReadBlob(
                        blobId2,
                        group2,
                        offsets,
                        TGuardedSgList(blobContent.GetBlocks()));
                    UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(2)), blobContent.GetBlock(0).AsStringBuf());
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldReturnExactlyOneVersionOfEachBlockInDescribeBlocksResponse) {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // Without fresh blocks.
        partition.WriteBlocks(0, char(1));
        partition.Flush();
        partition.CreateCheckpoint("checkpoint1");
        partition.WriteBlocks(0, char(2));
        partition.Flush();
        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::MakeOneBlock(0));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.FreshBlockRangesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());
            const auto& blobPiece = response->Record.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece.RangesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlockIndex());

            const auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
            const auto group = blobPiece.GetBSGroupId();
            {
                TVector<ui16> offsets{0};

                TBlockBuffer blobContent;
                for (size_t i = 0; i < offsets.size(); ++i) {
                    blobContent.AddBlock(DefaultBlockSize, 0);
                }

                const auto response = partition.ReadBlob(
                    blobId,
                    group,
                    offsets,
                    TGuardedSgList(blobContent.GetBlocks()));
                UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(2)), blobContent.GetBlock(0).AsStringBuf());
            }
        }

        // With fresh blocks.
        partition.WriteBlocks(0, char(3));
        partition.CreateCheckpoint("checkpoint2");
        partition.WriteBlocks(0, char(4));
        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::MakeOneBlock(0));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.BlobPiecesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.FreshBlockRangesSize());
            const auto& freshBlockRange = response->Record.GetFreshBlockRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(1, freshBlockRange.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(char(4)), freshBlockRange.GetBlocksContent());
        }
    }

    Y_UNIT_TEST(ShouldIgnoreZeroedBlocksWhileDescribingBlocks) {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.ZeroBlocks(1);
        partition.Flush();
        partition.Compaction();
        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.FreshBlockRangesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());
            const auto& blobPiece = response->Record.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece.RangesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlockIndex());

            const auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blobId.BlobSize());
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorWhenReadingFromUnknownCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendReadBlocksRequest(
            TBlockRange32::MakeOneBlock(0),
            "unknown");

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldZeroBlocksOnOverlayDisk)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        partition.WriteBlocks(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetBlocksContent(1), GetBlockContent(partition.ReadBlocks(1)));

        partition.ZeroBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL("", GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldZeroBlocksOnOverlayDiskAfterFlush)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        partition.WriteBlocks(1, 1);
        partition.Flush();
        UNIT_ASSERT_VALUES_EQUAL(GetBlocksContent(1), GetBlockContent(partition.ReadBlocks(1)));

        partition.ZeroBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL("", GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldCorrectlyOverwriteZeroedBlocksOnOverlayDisk)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        partition.ZeroBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL("", GetBlockContent(partition.ReadBlocks(1)));

        partition.WriteBlocks(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetBlocksContent(1), GetBlockContent(partition.ReadBlocks(1)));

        partition.Flush();
        partition.Cleanup();
        partition.Compaction();
        UNIT_ASSERT_VALUES_EQUAL(GetBlocksContent(1), GetBlockContent(partition.ReadBlocks(1)));
    }

    Y_UNIT_TEST(ShouldCorrectlyCompactZeroedBlocksOnOverlayDisk)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.ZeroBlocks(1);
        partition.Flush();
        partition.Cleanup();
        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL("", GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(GetBlocksContent(2), GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldCorrectlyCompactSmallZeroBlobsOnOverlayDisk)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        TString expected;
        for (size_t i = 0; i <= MaxBlocksCount; ++i) {
            if (i % 2 == 0) {
                expected += GetBlockContent(1);
                partition.WriteBlocks(i, 1);
                partition.Flush();
                partition.Cleanup();
            } else {
                expected += GetBlockContent(0);
                partition.ZeroBlocks(i);
            }
        }
        partition.Compaction();

        auto actual = GetBlocksContent(partition.ReadBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount + 1)));
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(ShouldCorrectlyCompactLargeZeroBlobsOnOverlayDisk)
    {
        auto setup = SetupOverlayPartition(TestTabletId, TestTabletId2);
        auto& partition = *setup.Partition;

        TString expected;
        for (size_t i = 0; i < MaxBlocksCount/2; ++i) {
            expected += GetBlockContent(0);
            partition.ZeroBlocks(i);
        }
        for (size_t i = MaxBlocksCount/2; i < MaxBlocksCount; ++i) {
            expected += GetBlocksContent(1);
            partition.WriteBlocks(i, 1);
        }
        for (size_t i = MaxBlocksCount; i <= MaxBlocksCount; ++i) {
            expected += GetBlocksContent(0);
            partition.ZeroBlocks(i);
        }
        partition.Flush();
        partition.Cleanup();
        partition.Compaction();

        auto actual = GetBlocksContent(partition.ReadBlocks(
            TBlockRange32::WithLength(0, MaxBlocksCount + 1)));
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(ShouldReadBlocksFromBaseDisk)
    {
        TPartitionContent baseContent = {
        /*|      0     |     1    |    2 ... 5    |    6    |      7     |     8    |     9     |*/
            Blob(1, 1) , Fresh(2) , Blob(2, 3, 4) , Empty() , Blob(1, 4) , Fresh(5) , Blob(2, 6)
        };
        auto bitmap = CreateBitmap(10);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 10));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(baseContent), GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksFromOverlayDisk)
    {
        TPartitionContent baseContent = {
            Blob(1, 1), Fresh(2), Blob(2, 3)
        };
        auto bitmap = CreateBitmap(3);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        const auto range = TBlockRange32::WithLength(0, baseContent.size());

        partition.WriteBlocks(range, char(2));
        MarkWrittenBlocks(bitmap, range);

        partition.Flush();

        auto response = partition.ReadBlocks(range);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(2), range.Size()),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksFromOverlayDiskWhenRangeOverlapsWithBaseDisk)
    {
        TPartitionContent baseContent = {
        /*|      0     |     1    |      2     |    3    |      4     |     5    |      6    |*/
            Blob(1, 1) , Fresh(1) , Blob(2, 2) , Empty() , Blob(3, 3) , Fresh(3) , Blob(4, 3)
        };
        auto bitmap = CreateBitmap(7);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        auto writeRange = TBlockRange32::WithLength(2, 3);
        partition.WriteBlocks(writeRange, char(4));
        MarkWrittenBlocks(bitmap, writeRange);

        partition.Flush();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 7));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(1), 2) +
            GetBlocksContent(char(4), 3) +
            GetBlocksContent(char(3), 2),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldNotReadBlocksFromBaseDiskWhenTheyAreZeroedInOverlayDisk)
    {
        TPartitionContent baseContent = {
            Blob(1, 1), Blob(1, 2), Blob(1, 3)
        };
        auto bitmap = CreateBitmap(3);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        const auto range = TBlockRange32::WithLength(0, baseContent.size());

        partition.ZeroBlocks(range);

        partition.Flush();

        auto response = partition.ReadBlocks(range);

        UNIT_ASSERT_VALUES_EQUAL(TString(), GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksWithBaseDiskAndComplexRanges)
    {
        TPartitionContent baseContent = {
        /*|     0   |      1     |     2    |    3    |     4   |    5    |      6     |     7    |    8   |*/
            Empty() , Blob(1, 1) , Fresh(2) , Empty() , Empty() , Empty() , Blob(2, 3) , Fresh(4) , Empty()
        };
        auto bitmap = CreateBitmap(9);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        auto writeRange1 = TBlockRange32::WithLength(2, 2);
        partition.WriteBlocks(writeRange1, char(5));
        MarkWrittenBlocks(bitmap, writeRange1);

        auto writeRange2 = TBlockRange32::WithLength(5, 2);
        partition.WriteBlocks(writeRange2, char(6));
        MarkWrittenBlocks(bitmap, writeRange2);

        partition.Flush();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 9));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(0), 1) +
            GetBlocksContent(char(1), 1) +
            GetBlocksContent(char(5), 2) +
            GetBlocksContent(char(0), 1) +
            GetBlocksContent(char(6), 2) +
            GetBlocksContent(char(4), 1) +
            GetBlocksContent(char(0), 1),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterCompactionOfOverlayDisk1)
    {
        TPartitionContent baseContent = { Blob(1, 1) };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Write 1023 blocks (4MB minus 4KB). After compaction, we have one
        // merged blob written.
        // It's a tricky situation because one block (at 0 index) is missed in
        // overlay disk and therefore this block should be read from base disk.
        auto writeRange = TBlockRange32::WithLength(1, 1024);
        partition.WriteBlocks(writeRange, char(2));
        MarkWrittenBlocks(bitmap, writeRange);

        auto response =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)) +
            GetBlocksContent(char(2), 1023),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterCompactionOfOverlayDisk2)
    {
        TPartitionContent baseContent;
        for (size_t i = 0; i < 1000; ++i) {
            baseContent.push_back(Empty());
        }
        for (size_t i = 1000; i < 1024; ++i) {
            baseContent.push_back(Blob(i, 1));
        }
        auto bitmap = CreateBitmap(1024);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Write 1000 blocks. After compaction, we have one merged blob written.
        // It's a tricky situation because some blocks (at [1000..1023] range)
        // are missed in overlay disk and therefore this blocks should be read
        // from base disk.
        auto writeRange = TBlockRange32::WithLength(0, 1000);
        partition.WriteBlocks(writeRange, char(2));
        MarkWrittenBlocks(bitmap, writeRange);

        partition.Compaction();

        auto response =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(2), 1000) +
            GetBlocksContent(char(1), 24),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterZeroBlocksAndCompactionOfOverlayDisk)
    {
        TPartitionContent baseContent = { Blob(1, 1) };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Zero 1023 blocks. After compaction, we have one merged zero blob
        // written. It's a tricky situation because one block (at 0 index) is
        // missed in overlay disk and therefore this block should be read from
        // base disk.
        auto zeroRange = TBlockRange32::MakeClosedInterval(1, 1023);
        partition.ZeroBlocks(zeroRange);
        MarkZeroedBlocks(bitmap, zeroRange);

        partition.Compaction();

        auto response =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)) +
            GetBlocksContent(char(0), 1023),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldNotKillTabletWhenFailedToReadBlobFromBaseDisk)
    {
        TPartitionContent baseContent = { Blob(1, 1) };

        const auto baseTabletId = TestTabletId2;

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, baseTabletId, baseContent);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        int pillCount = 0;

        const auto eventHandler =
            [&] (const TEvBlobStorage::TEvGet::TPtr& ev) {
                bool result = false;

                auto& msg = *ev->Get();

                auto response = std::make_unique<TEvBlobStorage::TEvGetResult>(
                    NKikimrProto::ERROR,
                    msg.QuerySize,
                    0);  // groupId

                for (ui32 i = 0; i < msg.QuerySize; ++i) {
                    const auto& q = msg.Queries[i];
                    const auto& blobId = q.Id;

                    if (blobId.TabletID() == baseTabletId) {
                        result = true;
                    }
                }

                if (result) {
                    runtime.Schedule(
                        new IEventHandle(
                            ev->Sender,
                            ev->Recipient,
                            response.release(),
                            0,
                            ev->Cookie),
                        TDuration());
                }

                return result;
            };

        runtime.SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped =
                    [&] (const auto& ev) {
                        handled = eventHandler(ev);
                    };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, wrapped);
                }
                return handled;
           });

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvents::TSystem::PoisonPill: {
                        ++pillCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendReadBlocksRequest(0);

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT_VALUES_EQUAL(0, pillCount);
    }

    Y_UNIT_TEST(ShouldReadBlocksWhenBaseBlobHasGreaterChannel)
    {
        const ui32 overlayTabletChannelsCount = 253;
        const ui32 baseBlobChannel = overlayTabletChannelsCount + 1;

        TPartitionContent baseContent = {
            Blob(1, 1, 1, baseBlobChannel)
        };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(
                TestTabletId,
                TestTabletId2,
                baseContent,
                overlayTabletChannelsCount);
        auto& partition = *partitionWithRuntime.Partition;

        auto response = partition.ReadBlocks(0);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldSendBlocksCountToReadInDescribeBlocksRequest)
    {
        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, {});
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        partition.WriteBlocks(4, 1);
        partition.WriteBlocks(5, 1);

        int describeBlocksCount = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvDescribeBlocksRequest: {
                        auto* msg = event->Get<TEvVolume::TEvDescribeBlocksRequest>();
                        auto& record = msg->Record;
                        UNIT_ASSERT_VALUES_EQUAL(7, record.GetBlocksCountToRead());
                        ++describeBlocksCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.ReadBlocks(TBlockRange32::WithLength(0, 9));
        UNIT_ASSERT_VALUES_EQUAL(1, describeBlocksCount);
    }

    Y_UNIT_TEST(ShouldCorrectlyGetChangedBlocksForOverlayDisk)
    {
        TPartitionContent baseContent = {
        /*|      0     |     1    |     2 ... 5   |     6    |      7     |     8    |      9     |*/
            Blob(1, 1) , Fresh(2) , Blob(2, 3, 4) ,  Empty() , Blob(1, 4) , Fresh(5) , Blob(2, 6)
        };

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);

        auto& partition = *partitionWithRuntime.Partition;
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(2, 2);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "cp1",
                "cp2");

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask().size(), 128);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], char(0b00000110));
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[1], 0);
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "cp2");

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask().size(), 128);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[0], char(0b10111111));
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetMask()[1], char(0b00000011));
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyGetChangedBlocksWhenRangeIsOutOfBounds)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 10);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 1);
        partition.CreateCheckpoint("cp");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::MakeClosedInterval(10, 1023),
                "",
                "cp");
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask().size());
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "cp");

            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b00000110),
                response->Record.GetMask()[0]
            );
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b00000000),
                response->Record.GetMask()[1]
            );
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorIfCompactionIsAlreadyRunning)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024,1024));

        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        if (!addBlobs) {
                            addBlobs = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        partition.SendCompactionRequest(0);

        partition.SendCompactionRequest(1024);

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(FAILED(compactResponse->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, compactResponse->GetStatus());
        }

        runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        runtime->Send(addBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(SUCCEEDED(compactResponse->GetStatus()));
        }

        {
            partition.SendCompactionRequest(0);
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldStartCompactionOnCompactRagesRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendCompactRangeRequest(0, 100);

        auto response = partition.RecvCompactRangeResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetOperationId().empty(), false);
    }

    Y_UNIT_TEST(ShouldReturnErrorForUnknownIdInCompactionStatusRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendGetCompactionStatusRequest("xxx");

        auto response = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldReturnCompactionProgress)
    {
        auto runtime = PrepareTestActorRuntime();

        TActorId rangeActor;
        TActorId partActor;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        rangeActor = event->Sender;
                        partActor = event->Recipient;
                        return TTestActorRuntime::EEventAction::DROP ;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        partition.SendGetCompactionStatusRequest(compResponse->Record.GetOperationId());

        auto statusResponse1 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse1->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(statusResponse1->Record.GetIsCompleted(), false);
        UNIT_ASSERT_VALUES_EQUAL(statusResponse1->Record.GetTotal() , 1);

        auto compactionResponse = std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>();
        runtime->Send(
            new IEventHandle(
                rangeActor,
                partActor,
                compactionResponse.release(),
                0, // flags
                0),
            0);

        partition.SendGetCompactionStatusRequest(compResponse->Record.GetOperationId());

        auto statusResponse2 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse2->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(statusResponse2->Record.GetIsCompleted(), true);
        UNIT_ASSERT_VALUES_EQUAL(statusResponse2->Record.GetTotal(), 1);
    }

    Y_UNIT_TEST(ShouldReturnCompactionStatusForLastCompletedCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        auto statusResponse = partition.GetCompactionStatus(
            compResponse->Record.GetOperationId());

        UNIT_ASSERT_VALUES_EQUAL(statusResponse->Record.GetIsCompleted(), true);
        UNIT_ASSERT_VALUES_EQUAL(statusResponse->Record.GetTotal(), 1);
    }

    Y_UNIT_TEST(ShouldUseWriteBlobThreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetWriteBlobThresholdSSD(128_KB);
        TTestPartitionInfo testPartitionInfo;

        const auto mediaKinds = {
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };

        for (auto mediaKind: mediaKinds) {
            testPartitionInfo.MediaKind = mediaKind;
            auto runtime = PrepareTestActorRuntime(
                config,
                1024,
                {},
                testPartitionInfo,
                {},
                EStorageAccessMode::Default
            );

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(TBlockRange32::WithLength(0, 255));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 255);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
            }

            partition.WriteBlocks(TBlockRange32::WithLength(0, 256));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 255);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 256);
            }
        }

        {
            testPartitionInfo.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
            auto runtime = PrepareTestActorRuntime(
                config,
                1024,
                {},
                testPartitionInfo,
                {},
                EStorageAccessMode::Default
            );

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(TBlockRange32::WithLength(0, 31));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 31);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
            }

            partition.WriteBlocks(TBlockRange32::WithLength(0, 32));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 31);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 32);
            }
        }
    }

    Y_UNIT_TEST(ShouldInitializeGarbageQueueOnStartup)
    {
        auto config = DefaultConfig();
        config.SetCollectGarbageThreshold(999999);
        config.SetSSDV2MaxBlobsPerRange(999999);
        config.SetHDDV2MaxBlobsPerRange(999999);
        config.SetCompactionGarbageThreshold(999999);
        config.SetDontEnqueueCollectGarbageUponPartitionStartup(true);

        auto runtime = PrepareTestActorRuntime(
            config,
            2048,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            1024
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // adding a new zone-local blob
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // resetting state (garbage queue should be reinitialized after this)
        partition.RebootTablet();

        // adding a global blob
        partition.WriteBlocks(TBlockRange32::WithLength(512, 1024));

        // running collect garbage hard
        partition.CollectGarbage();

        // running collect garbage soft - both blobs should be marked with a keep flag
        partition.CollectGarbage();

        // deleting garbage
        partition.DeleteGarbage();

        // reading data
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
    }

    Y_UNIT_TEST(ShouldHandleHttpCollectGarbage)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto createResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(createResponse->Html.Contains("Operation successfully completed"), true);
    }

    Y_UNIT_TEST(ShouldFailHttpGetCollectGarbage)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto createResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}));

        UNIT_ASSERT_C(createResponse->Html.Contains("Wrong HTTP method"), true);
    }

    Y_UNIT_TEST(ShouldFailHttpCollectGarbageOnTabletRestart)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool patchRequest = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvCollectGarbageRequest) {
                    if (patchRequest) {
                        patchRequest = false;
                        auto request = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();
                        SendUndeliverableRequest(*runtime, event, std::move(request));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto httpResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(httpResponse->Html.Contains("tablet is shutting down"), true);
    }

    Y_UNIT_TEST(ShouldNotDeleteBlobsMarkedAsGarbageByUncommittedTransactions)
    {
        auto config = DefaultConfig();
        config.SetCollectGarbageThreshold(999999);
        config.SetSSDV2MaxBlobsPerRange(999999);
        config.SetHDDV2MaxBlobsPerRange(999999);
        config.SetCompactionGarbageThreshold(999999);
        config.SetUpdateBlobsThreshold(999999);
        config.SetDontEnqueueCollectGarbageUponPartitionStartup(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // forcing hard gc to make sure that our next gc is soft
        partition.CollectGarbage();

        TVector<TPartialBlobId> blobIds;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        if (msg->Id.Channel() >= TPartitionSchema::FirstDataChannel) {
                            blobIds.push_back(MakePartialBlobId(msg->Id));
                        }

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // blob1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        // blob2
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);

        UNIT_ASSERT_VALUES_EQUAL(2, blobIds.size());

        // setting keep flags
        partition.CollectGarbage();

        // blob3 (making sure that our next CollectGarbage call does something)
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);

        bool evPutObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        Y_UNUSED(msg);
                        evPutObserved = true;

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // blob1 should be added to GarbageQueue
        partition.SendCleanupRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(evPutObserved);

        bool collectGarbageResultObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                        Y_UNUSED(msg);

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->DoNotKeep) {
                            UNIT_ASSERT_VALUES_EQUAL(0, msg->DoNotKeep->size());
                        }
                        collectGarbageResultObserved = true;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // CollectGarbage should not delete blob1
        partition.SendCollectGarbageRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(collectGarbageResultObserved);

        // Effectively aborting Cleanup transaction
        partition.RebootTablet();

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        // Ensuring that our index is initialized
        partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));
        // Running garbage compaction - it should succeed reading blob1
        partition.Compaction(
            TEvPartitionPrivate::ECompactionMode::GarbageCompaction,
            TVector<TPartialBlobId>({blobIds[0]})
        );
    }

    Y_UNIT_TEST(ShouldForgetTooOldCompactRangeOperations)
    {
        constexpr TDuration CompactOpHistoryDuration = TDuration::Days(1);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TVector<TString> op;

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.WriteBlocks(0, 100);
            partition.SendCompactRangeRequest(0, 100);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvPartitionPrivate::EvForcedCompactionCompleted,
                    1);
                runtime->DispatchEvents(options);
            }

            auto response = partition.RecvCompactRangeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            op.push_back(response->Record.GetOperationId());
        }

        runtime->AdvanceCurrentTime(CompactOpHistoryDuration + TDuration::Seconds(1));

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.SendGetCompactionStatusRequest(op[i]);
            auto response = partition.RecvGetCompactionStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldDrain)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // drain before any requests should work
        partition.Drain();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));       // fresh
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));   // blob
        partition.ZeroBlocks(TBlockRange32::MakeOneBlock(0));        // fresh
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));    // blob

        // drain after some requests have completed should work
        partition.Drain();

        TDeque<std::unique_ptr<IEventHandle>> evPutRequests;
        bool intercept = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (intercept) {
                    switch (event->GetTypeRewrite()) {
                        case TEvBlobStorage::EvPutResult: {
                            evPutRequests.emplace_back(event.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        auto test = [&] (TString testName, bool isWrite)
        {
            runtime->DispatchEvents({}, TDuration::Seconds(1));

            // sending this request after DispatchEvents because our pipe client
            // reconnects from time to time and thus fifo guarantee for
            // zero/write -> drain is broken => we need to ensure the correct
            // order (zero/write, then drain) in our test scenario
            partition.SendDrainRequest();

            UNIT_ASSERT_C(
                evPutRequests.size(),
                TStringBuilder() << testName << ": intercepted EvPut requests"
            );

            auto evList = runtime->CaptureEvents();
            for (auto& ev: evList) {
                UNIT_ASSERT_C(
                    ev->GetTypeRewrite() != TEvPartition::EvDrainResponse,
                    TStringBuilder() << testName << ": check no drain response"
                );
            }
            runtime->PushEventsFront(evList);

            intercept = false;

            for (auto& request: evPutRequests) {
                runtime->Send(request.release());
            }

            evPutRequests.clear();

            runtime->DispatchEvents({}, TDuration::Seconds(1));

            if (isWrite) {
                {
                    auto response = partition.RecvWriteBlocksResponse();
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                }
            } else {
                {
                    auto response = partition.RecvZeroBlocksResponse();
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                }
            }

            {
                auto response = partition.RecvDrainResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response->GetStatus(),
                    TStringBuilder() << testName << ": check drain response"
                );
            }

            intercept = true;
        };

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0));
        test("write fresh", true);

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
        test("write blob", true);

        partition.SendZeroBlocksRequest(TBlockRange32::MakeOneBlock(0));
        test("zero fresh", false);

        partition.SendZeroBlocksRequest(TBlockRange32::WithLength(0, 1024));
        test("zero blob", false);
    }

    Y_UNIT_TEST(ShouldProperlyHandleCollectGarbageErrors)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        bool channel3requestObserved = false;
        bool channel4requestObserved = false;
        bool channel4responseObserved = false;
        bool deleteGarbageObserved = false;
        bool sendError = true;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (3 == msg->Channel) {
                            channel3requestObserved = true;
                            if (sendError) {
                                auto response =
                                    std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(
                                        NKikimrProto::ERROR,
                                        0,  // doesn't matter
                                        0,  // doesn't matter
                                        0,  // doesn't matter
                                        msg->Channel
                                    );

                                runtime.Send(new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    0
                                ), 0);

                                return TTestActorRuntime::EEventAction::DROP;
                            }
                        } else if (4 == msg->Channel) {
                            channel4requestObserved = true;
                        }

                        break;
                    }

                    case TEvBlobStorage::EvCollectGarbageResult: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbageResult>();
                        if (4 == msg->Channel) {
                            channel4responseObserved = true;
                        }

                        break;
                    }

                    case TEvPartitionPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageObserved = true;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // 10 blobs needed to trigger automatic collect
        for (ui32 i = 0; i < 9; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }
        partition.Compaction();
        partition.Cleanup();
        partition.SendCollectGarbageRequest();
        {
            auto response = partition.RecvCollectGarbageResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_KIKIMR_ERROR(NKikimrProto::ERROR),
                response->GetStatus()
            );
        }
        UNIT_ASSERT(channel3requestObserved);
        UNIT_ASSERT(channel4requestObserved);
        UNIT_ASSERT(channel4responseObserved);
        UNIT_ASSERT(!deleteGarbageObserved);
        channel3requestObserved = false;
        channel4requestObserved = false;
        channel4responseObserved = false;
        sendError = false;

        runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT(channel3requestObserved);
        UNIT_ASSERT(channel4requestObserved);
        UNIT_ASSERT(channel4responseObserved);
        UNIT_ASSERT(deleteGarbageObserved);
    }

    Y_UNIT_TEST(ShouldExecuteCollectGarbageAtStartup)
    {
        const auto channelCount = 7;
        const auto groupCount = channelCount - DataChannelOffset;

        auto isDataChannel = [&] (ui64 ch) {
            return (ch >= DataChannelOffset) && (ch < channelCount);
        };

        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetRunV2SoftGcAtStartup(true);

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        const auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount, config);

        TPartitionClient partition(runtime, 0, tabletId);

        ui32 gcRequests = 0;
        bool deleteGarbageSeen = false;

        NActors::TActorId gcActor = {};

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->TabletId == tabletId &&
                            msg->Hard == false &&
                            isDataChannel(msg->Channel))
                        {
                            if (!gcActor || gcActor == event->Sender) {
                                gcActor = event->Sender;
                                ++gcRequests;
                            }
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageSeen = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPartitionPrivate::EvCollectGarbageResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(gcRequests, 3);
        UNIT_ASSERT_VALUES_EQUAL(deleteGarbageSeen, false);
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleTabletInfoChannelCountMoreThanConfigChannelCount)
    {
        constexpr ui32 channelCount = 7;
        constexpr ui32 tabletInfoChannelCount = 11;

        TTestEnv env(0, 1, tabletInfoChannelCount, 4);
        auto& runtime = env.GetRuntime();

        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            tabletInfoChannelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        for (ui32 i = 0; i < 30; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }

        partition.Compaction();
        partition.CollectGarbage();
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleTabletInfoChannelCountLessThanConfigChannelCount)
    {
        constexpr ui32 channelCount = 11;
        constexpr ui32 tabletInfoChannelCount = 7;

        TTestEnv env(0, 1, tabletInfoChannelCount, 4);
        auto& runtime = env.GetRuntime();

        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            tabletInfoChannelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        for (ui32 i = 0; i < 30; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }

        partition.Compaction();
        partition.CollectGarbage();
    }

    Y_UNIT_TEST(ShouldMarkFreshBlocksDeletedViaFreshBlockUpdates)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0 .. 5 .. 10 .. 15 .. 20 .. 25 .. 30 .. 35
        // *--fresh--*
        //                  *---fresh---*
        //      *zero*
        //                        *-----merged-----*
        //                                   *fresh*
        //
        //                   FLUSH
        //
        // *zero*           *zero*           *zero*

        partition.WriteBlocks(TBlockRange32::WithLength(0, 10), 1);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(15, 24), 2);
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5, 9));
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(20, 34), 3);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(30, 34), 4);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 25);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 15);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
        }

        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 35));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(char(1), 5) +
                GetBlocksContent(char(0), 10) +
                GetBlocksContent(char(2), 5) +
                GetBlocksContent(char(3), 10) +
                GetBlocksContent(char(4), 5),
                GetBlocksContent(response));
        }

        partition.RebootTablet();
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 40);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }

        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 35));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(char(1), 5) + GetBlocksContent(char(0), 10) +
                    GetBlocksContent(char(2), 5) +
                    GetBlocksContent(char(3), 10) +
                    GetBlocksContent(char(4), 5),
                GetBlocksContent(response));
        }

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 5));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(15, 19));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(30, 34));

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 15);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
        }

        {
            auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 35));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(char(0), 20) +
                GetBlocksContent(char(3), 10) +
                GetBlocksContent(char(0), 5),
                GetBlocksContent(response));
        }
    }

    Y_UNIT_TEST(ShouldDelayFlushUntilInFlightFreshBlocksComplete)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1);
        partition.WriteBlocks(2);

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(3);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(evAddFreshBlocksSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
        }

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 0);
        }

        UNIT_ASSERT(evAddFreshBlocks);
        runtime->Send(evAddFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
        {
            auto response = partition.RecvFlushResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 3);
        }
    }

    Y_UNIT_TEST(ShouldDelayCleanupUntilInFlightFreshBlocksComplete)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0        20    25   30   35
        // *-------------------------*
        // *--------------*
        //                 *ooooooooo*   <-- delayed
        //          *-----------*
        //                 *xxxxxxxxx*   <-- completed


        partition.WriteBlocks(TBlockRange32::WithLength(0, 35));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 25));

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(25, 10));

        partition.WriteBlocks(TBlockRange32::WithLength(20, 10));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(evAddFreshBlocksSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 35 + 25);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }

        partition.SendCleanupRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 35 + 25);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }

        UNIT_ASSERT(evAddFreshBlocks);
        runtime->Send(evAddFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
        {
            auto response = partition.RecvCleanupResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10 + 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 20);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);
        }
    }

    Y_UNIT_TEST(ShouldDelayCreateCheckpointUntilInFlightFreshBlocksComplete)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0    15   20    25   30   35                                content:
        //
        // *-------------------------*   <-- merged #1                     (1)
        // *--------------*              <-- merged #2                     (2)
        //                 *ooooooooo*   <-- delayed fresh #1              (3)
        //           *----------*        <-- fresh #2                      (4)
        //
        // ------------ CreateCheckpoint ------------
        //
        //
        //      *--------------------*   <-- merged #3 that overlaps       (5)
        //                                   fresh from prev chunk
        //                                   and a piece of merged #2
        //
        //                 *xxxxxxxxx*   <-- completed fresh #1; should
        //                                   be put inside the prev chunk
        //
        //  ------------   Cleanup    ------------
        //
        //  should remove merged #1 blob

        // merged #1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 35), 1);
        // merged #2
        partition.WriteBlocks(TBlockRange32::WithLength(0, 25), 2);

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // delayed fresh #1
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(25, 10), 3);
        // fresh #2
        partition.WriteBlocks(TBlockRange32::WithLength(20, 10), 4);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(evAddFreshBlocksSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 35 + 25);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);
        }

        // checkpoint1. should wait for fresh to complete
        partition.SendCreateCheckpointRequest("checkpoint1");

        // merged #3
        partition.WriteBlocks(TBlockRange32::WithLength(15, 20), 5);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 35 + 25 + 20);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 3);
        }

        // release delayed fresh #1
        UNIT_ASSERT(evAddFreshBlocks);
        runtime->Send(evAddFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }
        {
            auto response = partition.RecvCreateCheckpointResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // it's okay that checkpoint blocks count is zero
            // becase we haven't touched the blobs yet
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10 + 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 35 + 25 + 20);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 3);
        }

        partition.Cleanup();
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetCheckpointBlocksCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 10 + 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 25 + 20);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 2);

        }

        // Read from latest commitId to verify block consistency
        {
            auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 35));

            TString expected =
                GetBlocksContent(char(2), 15) +
                GetBlocksContent(char(5), 20);

            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }

        // Read from checkpoint to verify checkpoint consistency
        {
            auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 35), "checkpoint1");
            TString expected =
                GetBlocksContent(char(2), 20) +
                GetBlocksContent(char(4), 10) +
                GetBlocksContent(char(3), 5);

            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }

        }
    }

    Y_UNIT_TEST(ShouldNotRebaseBlockIfThereIsFreshBlockInFlightWithTheSameBlockIndex)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0   5   10   15
        // *--------*                  <-- fresh #1
        //
        //  ---- flush start   -----
        //
        //     *oooo*                  <-- delayed fresh #2
        //
        //                     *---*   <-- zero #1 just to generate commitId
        //                                 higher than fresh #2 commitId;
        //                                 could be also any operation that
        //                                 generates commit id
        //
        //  ---- addblob start -----
        //  ---- addblob end   -----
        //  ---- flush   end   -----
        //
        //     *xxxx*                  <-- fresh #2 completed

        // fresh #1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 10), 1);

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        TAutoPtr<IEventHandle> evAddBlobs;
        bool evAddBlobsSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddBlobsRequest && !evAddBlobsSeen) {
                    evAddBlobs = event.Release();
                    evAddBlobsSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // flush start
        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(evAddBlobsSeen);

        // fresh #2 start
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(5, 5), 2);

        // zero #1 just to generate commitID higher than fresh #2 one
        partition.ZeroBlocks(20);

        // flush finish
        runtime->Send(evAddBlobs.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.RecvFlushResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // fresh #2 finish
        runtime->Send(evAddFreshBlocks.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // check block content
        {
            auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 10));

            TString expected =
                GetBlocksContent(char(1), 5) +
                GetBlocksContent(char(2), 5);

            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldFillEnryptedBlockMaskWhenReadBlock)
    {
        auto range = TBlockRange32::WithLength(0, 16);
        auto emptyBlock = TString::Uninitialized(DefaultBlockSize);

        auto bitmap = CreateBitmap(16);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto writeRange = TBlockRange32::WithLength(1, 4);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRange = TBlockRange32::WithLength(5, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            auto zeroRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto zeroRange = TBlockRange32::WithLength(1, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRange = TBlockRange32::WithLength(5, 3);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            auto writeRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }
    }

    Y_UNIT_TEST(ShouldFillEnryptedBlockMaskWhenReadBlockFromOverlayDisk)
    {
        TPartitionContent baseContent = {
        /*|   0     |   1    |   2     |   3 ... 5    |    6    |   7    |   8    |    9    |   10...12    |   13    |   14   |   15    |*/
            Fresh(1), Empty(), Fresh(2), Blob(2, 3, 3), Fresh(4), Empty(), Empty(), Fresh(5), Blob(3, 6, 3), Fresh(7), Empty(), Fresh(8)
        };

        auto range = TBlockRange32::WithLength(0, 16);
        auto emptyBlock = TString::Uninitialized(DefaultBlockSize);

        auto bitmap = CreateBitmap(16);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        {
            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto writeRange = TBlockRange32::WithLength(1, 4);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRange = TBlockRange32::WithLength(5, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            auto zeroRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto zeroRange = TBlockRange32::WithLength(1, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRange = TBlockRange32::WithLength(5, 3);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            auto writeRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }
    }

    Y_UNIT_TEST(ShouldMarkDeletedBlocksOnBoot)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(120_KB); // 30 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.ZeroBlocks(1);
        partition.CreateCheckpoint("checkpoint1");
        partition.WriteBlocks(1, 2);

        partition.RebootTablet();

        auto response = partition.ReadBlocks(1, "checkpoint1");
        UNIT_ASSERT_VALUES_EQUAL("", GetBlockContent(response));
    }

    Y_UNIT_TEST(ShouldCollectBlobsForCleanupAfterInFlightFreshCompletes)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0   4 5   9   14 15  19       29
        // *--------------*                   merged #1 (1)
        //                  *-------------*   merged #2 (2)
        //
        //    Cleanup #1 to remove deletions from merged #1 and #2
        //
        //                  *---*             fresh #1 (3)
        // *ooooooo*                          fresh #2 starts (4)
        //
        //    Cleanup #2 starts and waits for fresh #2 to complete
        //
        // *xxxxxxx*                          fresh #2 completes
        //
        //    Cleanup #2 is resumed and finishes
        //
        //            Flush
        //
        //      *---*                         fresh #3 (5)
        //
        //            Cleanup #3

        // merged #1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);
        // merged #2
        partition.WriteBlocks(TBlockRange32::WithLength(15, 15), 2);
        // cleanup #1
        partition.Cleanup();
        // fresh #1
        partition.WriteBlocks(TBlockRange32::WithLength(15, 5), 3);

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // fresh #2 starts
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 10), 4);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(true, evAddFreshBlocksSeen);

        // cleanup #2 starts
        partition.SendCleanupRequest();
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // resume fresh #2
        runtime->Send(evAddFreshBlocks.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            // fresh #2 completes
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        {
            // cleanup #2 completes
            auto response = partition.RecvCleanupResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        partition.Flush();
        // fresh #3
        partition.WriteBlocks(TBlockRange32::WithLength(5, 5), 5);
        // cleanup #3
        partition.Cleanup();

        // check block content
        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

            TString expected = GetBlocksContent(char(4), 5);
            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldWriteFreshBlocksIntoMixedIndex)
    {
        // Disabled: NBS-2451
        return;

        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetEnableConversionIntoMixedIndexV2(true);
        config.SetHotZoneRequestCountFactor(1);
        config.SetColdZoneRequestCountFactor(1);
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0    4    9      14
        // *-----------------*             # merged #1 (1)
        //
        //      Convert into Mixed Index
        //
        // *xxxx*                          # fresh #1 start (2)
        //
        //      Compaction starts
        //
        // *oooo*                          # fresh #1 completes
        //
        //      Compaction completes
        //

        // merged #1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);
        partition.ReadBlocks(TBlockRange32::WithLength(0, 15));

        // convert zone 0 to mixed index
        {
            auto response = partition.UpdateIndexStructures();
            UNIT_ASSERT_VALUES_EQUAL(1, response->MixedZoneCount);
        }

        partition.Cleanup();
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 15);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlobsCount(), 1);

        }

        TAutoPtr<IEventHandle> evAddFreshBlocks;
        bool evAddFreshBlocksSeen = false;

        TAutoPtr<IEventHandle> evAddBlobs;
        bool evAddBlobsSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddFreshBlocksRequest && !evAddFreshBlocksSeen) {
                    evAddFreshBlocks = event.Release();
                    evAddFreshBlocksSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddBlobsRequest && !evAddBlobsSeen) {
                    evAddBlobs = event.Release();
                    evAddBlobsSeen = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // fresh #1 starts
        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 5), 2);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(true, evAddFreshBlocksSeen);

        partition.SendCompactionRequest(0);  // force compaction
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(true, evAddBlobsSeen);

        // resume fresh #2
        runtime->Send(evAddFreshBlocks.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            // fresh #2 completes
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // resume compaction
        runtime->Send(evAddBlobs.Release());
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            // compaction completes
            auto response = partition.RecvCompactionResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // check block content
        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

            TString expected = GetBlocksContent(char(2), 5);
            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldDropFlushedBlocksOnBoot)
    {
        // it looks like nothing bad is gonna happen if we load
        // the flushed fresh blocks on boot. but we shouldn't forget
        // that freshBlockUpdateis were trimmed on flush, thus
        // we will not mark the corresponding blocks as deleted ones

        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 2);

        // disable trimFreshLog
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionCommonPrivate::EvTrimFreshLogRequest) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();

        partition.CreateCheckpoint("xxx");
        partition.RebootTablet();
        partition.Cleanup();
        partition.Flush();

        // check block content
        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

            TString expected = GetBlocksContent(char(2), 5);
            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldInitLastDeletionIdCorrectly)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 14), 1);
        partition.RebootTablet();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.RebootTablet();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);
        partition.RebootTablet();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(3), 4);
    }

    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshLogOnFlush)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }

        bool trimSeen = false;
        bool trimCompletedSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));
                        trimCompletedSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
        UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
    }

    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshBlobsFromPreviousGeneration)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);
        }

        bool trimSeen = false;
        bool trimCompletedSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        trimCompletedSeen = true;
                        auto* msg = event->Get<TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.KillTablet();

        {
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);

            UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
            UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
        }
    }

    Y_UNIT_TEST(ShouldHandleBSErrorsOnInitFreshBlocksFromChannel)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            NKikimrProto::ERROR,
                            TLogoBlobID(),  // doesn't matter
                            TLogoBlobID(),  // doesn't matter
                            0);             // doesn't matter

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            }
        );

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldInitIndexOnWriteFreshBlocks)
    {
        const auto zoneBlockCount = 64;

        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            zoneBlockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool initIndexRequestSeen = false;
        TVector<TBlockRange32> initIndexRequestBlockRanges;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvInitIndexRequest) {
                    auto* msg = event->Get<TEvPartitionPrivate::TEvInitIndexRequest>();
                    initIndexRequestSeen = true;
                    initIndexRequestBlockRanges = msg->BlockRanges;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // zone 1
        auto zone1BlockRange = TBlockRange32::MakeOneBlock(0);
        partition.WriteBlocks(zone1BlockRange, 1);

        UNIT_ASSERT_VALUES_EQUAL(true, initIndexRequestSeen);
        UNIT_ASSERT_VALUES_EQUAL(1, initIndexRequestBlockRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(zone1BlockRange, initIndexRequestBlockRanges.front());

        initIndexRequestSeen = false;
        initIndexRequestBlockRanges.clear();

        // zone 2
        auto zone2BlockRange = TBlockRange32::MakeOneBlock(zoneBlockCount);
        partition.WriteBlocks(zone2BlockRange, 2);

        UNIT_ASSERT_VALUES_EQUAL(true, initIndexRequestSeen);
        UNIT_ASSERT_VALUES_EQUAL(1, initIndexRequestBlockRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(zone2BlockRange, initIndexRequestBlockRanges.front());

        initIndexRequestSeen = false;
        initIndexRequestBlockRanges.clear();

        // zone 1 again
        partition.WriteBlocks(zone1BlockRange, 3);

        // zone has been initialized already
        UNIT_ASSERT_VALUES_EQUAL(false, initIndexRequestSeen);

        // zone 2 and 3
        auto zone2and3BlockRange = TBlockRange32::MakeClosedInterval(
            zoneBlockCount * 2 - 5,
            zoneBlockCount * 2 + 5);
        partition.WriteBlocks(zone2and3BlockRange, 4);

        // zone 3 hasn't been initialized yet
        UNIT_ASSERT_VALUES_EQUAL(true, initIndexRequestSeen);
        UNIT_ASSERT_VALUES_EQUAL(1, initIndexRequestBlockRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(zone2and3BlockRange, initIndexRequestBlockRanges.front());
    }

    Y_UNIT_TEST(ShouldHandleCorruptedFreshBlobOnInitFreshBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            msg->Status,
                            msg->From,
                            msg->To,
                            msg->GroupId);

                        response->Responses = std::move(msg->Responses);

                        // corrupt
                        auto& buffer = response->Responses[0].Buffer;
                        std::memset(buffer.Detach(), 0, 4);

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            }
        );

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldNotMixBlobUpdatesByFreshFromDifferentZones)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction

        const ui32 zoneBlockCount = 60;

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {},
            {},
            EStorageAccessMode::Default,
            zoneBlockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // merged #1 (zoneId: 2, deletionID: 1)
        partition.WriteBlocks(TBlockRange32::WithLength(60, 60));

        // merged #2 (zoneId: 2, deletionId: 2)
        partition.WriteBlocks(TBlockRange32::WithLength(60, 60));

        partition.RebootTablet();
        partition.WaitReady();

        // merged #3 (zoneId: 1, deletionId: 1)
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);

        // fresh #1 (zoneId: 1, deletionId: 2)
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 2);

        // load the second zone after reboot
        // merged #4 (zoneId: 2, deletionId: 3)
        partition.WriteBlocks(TBlockRange32::WithLength(60, 60));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 60 * 3 + 15);
        }

        // cleanup the second zone
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMergedBlocksCount(), 60 + 15);
        }

        partition.Flush();
        partition.TrimFreshLog();

        partition.RebootTablet();
        partition.WaitReady();

        // add a blob update to disable cleanup fast path
        partition.WriteBlocks(TBlockRange32::WithLength(5, 5), 3);

        // rebase merged #3 blocks
        partition.Cleanup();

        // check block content
        {
            auto response =
                partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

            TString expected = GetBlocksContent(char(2), 5);
            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldAllocateNewChunkForEachCheckpointOnZoneInitialization)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(60_KB); // 15 blocks or more => merged
        config.SetFlushThreshold(1_GB); // disabling flush
        config.SetUpdateBlobsThreshold(999999); // disabling cleanup
        config.SetSSDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetHDDV2MaxBlobsPerRange(999999); // disabling compaction
        config.SetCompactionGarbageThreshold(999999); // disabling garbage compaction

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // 0      4              14
        //
        //      checkpoint #1
        //
        // *----------------------*   # merged_1
        //
        //      cleanup               # remove blobUpdates from DB
        //
        // *------*                   # fresh_1 generates RAM only deletion
        //
        //      checkpoint #2
        //
        //      tablet restart        # MUST separate chunk for both
        //                            # checkpoints on zone initialization
        //
        //      flush                 # turn fresh_1 into merged
        //
        // *------*                   # zero_1 to overwrite deletion by fresh_1
        //
        //      cleanup               # rebase merged blocks

        partition.CreateCheckpoint("checkpoint1");
        // merged_1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 15), 1);

        partition.Cleanup();

        // fresh_1
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 2);

        partition.CreateCheckpoint("checkpoint2");

        partition.RebootTablet();

        partition.Flush();

        // zero_1
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 5));

        partition.Cleanup();

        // check block content
        {
            auto response = partition.ReadBlocks(
                TBlockRange32::WithLength(0, 5),
                "checkpoint2");

            TString expected = GetBlocksContent(char(2), 5);
            TString actual = GetBlocksContent(response);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); ++i) {
                int e = static_cast<int>(expected[i]);
                int a = static_cast<int>(actual[i]);
                UNIT_ASSERT_VALUES_EQUAL(e, a);
            }
        }
    }

    Y_UNIT_TEST(ShouldNotKillTabletBeforeMaxReadBlobErrorsHappened)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetMaxReadBlobErrorsBeforeSuicide(5);

        auto r = PrepareTestActorRuntime(config);
        auto& runtime = *r;

        TPartitionClient partition(runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1001), 1);

        NKikimrProto::TLogoBlobID blobId;
        {
            auto response = partition.DescribeBlocks(
                TBlockRange32::WithLength(0, 1001),
                "");
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());
            blobId = response->Record.GetBlobPieces(0).GetBlobId();
        }

        ui32 readBlobCount = 0;
        bool readBlobShouldFail = true;

        const auto eventHandler = [&] (const TEvBlobStorage::TEvGet::TPtr& ev) {
            auto& msg = *ev->Get();

            for (ui32 i = 0; i < msg.QuerySize; i++) {
                NKikimr::TLogoBlobID expected = LogoBlobIDFromLogoBlobID(blobId);
                NKikimr::TLogoBlobID actual = msg.Queries[i].Id;

                if (expected.IsSameBlob(actual)) {
                    readBlobCount++;

                    if (readBlobShouldFail) {
                        auto response = std::make_unique<TEvBlobStorage::TEvGetResult>(
                            NKikimrProto::ERROR,
                            msg.QuerySize,
                            0);  // groupId

                        runtime.Schedule(
                            new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                response.release(),
                                0,
                                ev->Cookie),
                            TDuration());
                        return true;
                    }
                }
            }

            return false;
        };

        runtime.SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped = [&] (const auto& ev) {
                    handled = eventHandler(ev);
                };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, wrapped);
                }
                return handled;
            }
        );

        bool suicideHappened = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTablet::EEv::EvTabletDead: {
                        suicideHappened = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            }
        );

        for (ui32 i = 1; i < config.GetMaxReadBlobErrorsBeforeSuicide(); i++) {
            partition.SendReadBlocksRequest(0);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));

            UNIT_ASSERT_VALUES_EQUAL(i, readBlobCount);
            UNIT_ASSERT(!suicideHappened);
        }

        partition.SendReadBlocksRequest(0);
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetMaxReadBlobErrorsBeforeSuicide(),
            readBlobCount);
        UNIT_ASSERT(suicideHappened);
        suicideHappened = false;

        {
            TPartitionClient partition(runtime);
            partition.WaitReady();

            readBlobShouldFail = false;
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(partition.ReadBlocks(0)),
                GetBlockContent(1)
            );
            UNIT_ASSERT(!suicideHappened);
        }
    }

    Y_UNIT_TEST(ShouldRejectSmallWritesAfterReachingFreshByteCountHardLimit)
    {
        NProto::TStorageServiceConfig config;
        config.SetFreshByteCountHardLimit(8_KB);
        config.SetFlushThreshold(4_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());
        UNIT_ASSERT(
            HasProtoFlag(response->GetError().GetFlags(), NProto::EF_SILENT));

        partition.Flush();

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCancelRequestsOnTabletRestart)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime({}, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(0, 1024);
        partition.SendWriteBlocksRequest(range, 1);

        bool putSeen = false;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvPutResult: {
                    putSeen = true;
                    return true;
                }
            }

            return false;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return putSeen;
        };
        runtime->DispatchEvents(options);

        partition.RebootTablet();

        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(
            "tablet is shutting down",
            response->GetErrorReason());
    }

    // The partition 2 write operations in compaction are not asynchronous,
    // which is why the test is only for read requests.
    Y_UNIT_TEST(ShouldAbortCompactionIfReadBlobFailsWithDeadlineExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetBlobStorageAsyncRequestTimeoutHDD(TDuration::Seconds(1).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(3, 33);
        partition.Flush();

        ui32 failedReadBlob = 0;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
                const auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
                if (msg->Record.GetHandleClass() == NKikimrBlobStorage::AsyncRead &&
                    msg->Record.GetMsgQoS().HasDeadlineSeconds())
                {
                    return true;
                }
            } else if (ev->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters) {
                auto* msg =
                    ev->Get<TEvStatsService::TEvVolumePartCounters>();
                failedReadBlob =
                    msg->DiskCounters->Simple.ReadBlobDeadlineCount.Value;
            }
            return false;
        });

        partition.SendCompactionRequest();
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());

        runtime->AdvanceCurrentTime(TDuration::Seconds(15));

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, failedReadBlob);
    }

    Y_UNIT_TEST(ShouldAbortFlushIfWriteBlobFailsWithDeadlineExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetBlobStorageAsyncRequestTimeoutHDD(
            TDuration::Seconds(1).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(3, 33);

        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);

                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvVPut>();
                    if (msg->Record.GetHandleClass() ==
                            NKikimrBlobStorage::AsyncBlob &&
                        msg->Record.GetMsgQoS().HasDeadlineSeconds())
                    {
                        return true;
                    }
                }
                return false;
            });

        partition.SendFlushRequest();
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldAllowForcedCompactionRequestsInPresenseOfTabletCompaction)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvPartitionPrivate::EvCompactionCompleted &&
                steal)
            {
                steal = false;
                return true;
            }
            return false;
        });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions());
        partition.Compaction(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));
    }

    Y_UNIT_TEST(ShouldAllowOnlyOneForcedCompactionRequestAtATime)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvPartitionPrivate::EvCompactionCompleted &&
                steal)
            {
                steal = false;
                return true;
            }
            return false;
        });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));
        partition.SendCompactionRequest(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCheckRange)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        ui32 status = -1;
        ui32 error = -1;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        error = msg->GetStatus();
                        status = msg->Record.GetStatus().GetCode();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;

            const auto response = partition.CheckRange("id", idx, size);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
            runtime->DispatchEvents(options, TDuration::Seconds(3));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);
        };

        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldCheckRangeWithBrokenBlocks)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        ui32 status = -1;
        ui32 error = -1;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        status = msg->Record.GetStatus().GetCode();
                        error = msg->Record.GetError().GetCode();

                        break;
                    }
                    case TEvService::EvReadBlocksLocalResponse: {
                        using TEv = TEvService::TEvReadBlocksLocalResponse;

                        auto response = std::make_unique<TEv>(
                            MakeError(E_IO, "block is broken"));

                        runtime->Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;

            partition.SendCheckRangeRequest("id", idx, size);
            const auto response =
                partition.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

            UNIT_ASSERT_VALUES_EQUAL(E_IO, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);
        };
        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldSuccessfullyCheckRangeIfDiskIsEmpty)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 idx = 0;
        const ui32 size = 1;
        const auto response = partition.CheckRange("id", idx, size);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime->DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Record.GetStatus().GetCode());
    }

    Y_UNIT_TEST(ShouldntCheckRangeWithBigBlockCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        constexpr ui32 bytesPerStripe = 1024;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerStripe(bytesPerStripe);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 idx = 0;

        partition.SendCheckRangeRequest(
            "id",
            idx,
            bytesPerStripe / DefaultBlockSize + 1);
        const auto response =
            partition.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime->DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldGetSameChecksumsWhileCheckRangeSimmilarDisks)
    {
        constexpr ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition1(*runtime);
        TPartitionClient partition2(*runtime);

        partition1.WaitReady();
        partition2.WaitReady();

        const auto writeData = [&](TPartitionClient& partition)
        {
            partition.WriteBlocks(
                TBlockRange32::MakeClosedInterval(0, 1024 * 10),
                42);
            partition.WriteBlocks(
                TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 15),
                99);
        };

        writeData(partition1);
        writeData(partition2);

        const auto response1 = partition1.CheckRange("id", 0, 1024, true);
        const auto response2 = partition2.CheckRange("id", 0, 1024, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
        runtime->DispatchEvents(options, TDuration::Seconds(3));

        const auto& checksums1 = response1->Record.GetChecksums();
        const auto& checksums2 = response2->Record.GetChecksums();

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>(checksums1.begin(), checksums1.end()),
            TVector<ui32>(checksums2.begin(), checksums2.end()));
    }

    Y_UNIT_TEST(ShouldGetDifferentChecksumsWhileCheckRangeDifferentDisks)
    {
        constexpr ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition1(*runtime);
        TPartitionClient partition2(*runtime);

        partition1.WaitReady();
        partition2.WaitReady();

        partition1.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            42);

        partition2.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            99);

        const auto response1 = partition1.CheckRange("id", 0, 1024, true);
        const auto response2 = partition2.CheckRange("id", 0, 1024, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
        runtime->DispatchEvents(options, TDuration::Seconds(3));

        const auto& checksums1 = response1->Record.GetChecksums();
        const auto& checksums2 = response2->Record.GetChecksums();

        UNIT_ASSERT_VALUES_EQUAL(
            checksums1.size(),
            checksums2.size());

        ui32 totalChecksums = 0;
        ui32 differentChecksums = 0;
        for (int i = 0; i < checksums1.size(); ++i) {
            if (checksums1.at(i) != checksums2.at(i)) {
                ++differentChecksums;
            }
            ++totalChecksums;
        }

        UNIT_ASSERT(differentChecksums * 2 < totalChecksums);
    }

    Y_UNIT_TEST(ShouldReturnBlobsIdsOfFailedBlobsDuringReadIfRequested)
    {
        constexpr ui32 blobCount = 4;
        constexpr ui32 blockCount = 512 * blobCount;

        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(100_KB);
        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();
        {
            ui32 current_offset = 0;
            for (ui32 i = 0; i < blobCount; ++i) {
                const auto blockRange =
                    TBlockRange32::WithLength(current_offset, 512);
                partition.WriteBlocks(blockRange, 1);
                current_offset += 512;
            }
        }

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvReadBlobRequest: {
                        auto response = std::make_unique<
                            TEvPartitionPrivate::TEvReadBlobResponse>(
                            MakeError(E_IO, "Simulated blob read failure"));

                        runtime->Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TGuardedBuffer<TString> Buffer = TGuardedBuffer(
            TString::Uninitialized(blockCount * DefaultBlockSize));
        auto sgList = Buffer.GetGuardedSgList();
        auto sgListOrError =
            SgListNormalize(sgList.Acquire().Get(), DefaultBlockSize);

        UNIT_ASSERT(!HasError(sgListOrError));

        auto request = partition.CreateReadBlocksLocalRequest(
            TBlockRange32::WithLength(0, blockCount),
            sgListOrError.ExtractResult());

        request->Record.ShouldReportFailedRangesOnFailure = true;

        partition.SendToPipe(std::move(request));

        auto response = partition.RecvReadBlocksLocalResponse();
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            blobCount,
            response->Record.FailInfo.FailedRanges.size());
    }

    Y_UNIT_TEST(ShouldPartitionSendStatistics)
    {
        auto config = DefaultConfig();

        // Enable push scheme
        config.SetPullPartitionStatisticsFromVolume(true);

        auto runtime = PrepareTestActorRuntime(config);

        bool isPartitionSendStatistic = false;

        auto _ = runtime->AddObserver<
            TEvPartitionCommonPrivate::TEvGetPartCountersResponse>(
            [&](TEvPartitionCommonPrivate::TEvGetPartCountersResponse::TPtr& ev)
            {
                Y_UNUSED(ev);
                isPartitionSendStatistic = true;
            });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendToPipe(
            std::make_unique<
                TEvPartitionCommonPrivate::TEvGetPartCountersRequest>());

        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        runtime->DispatchEvents();

        // Check that partition sent statistics
        UNIT_ASSERT(isPartitionSendStatistic);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2

template <>
inline void Out<NCloud::NBlockStore::TBlockRange32>(
    IOutputStream& out,
    const NCloud::NBlockStore::TBlockRange32& range)
{
    out << "[" << range.Start << ", " << range.End << "]";
}
